package streamprocess.faulttolerance.checkpoint;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.shapshot.SnapshotResult;
import engine.shapshot.SnapshotStrategy;
import engine.table.datatype.serialize.Serialize;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.faulttolerance.FTManager;
import streamprocess.faulttolerance.FaultToleranceConstants;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;

import static UserApplications.CONTROL.*;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastCommitSnapshotResult;

public class CheckpointManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(CheckpointManager.class);
    public boolean running=true;
    private Path Current_Path;
    private FileSystem localFS;
    private File checkpointFile;
    private ExecutionGraph g;
    private Database db;
    private Configuration conf;
    private Object lock;
    private SnapshotResult snapshotResult;
    private boolean close;
    private Queue<Long> isCommitted;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callSnapshot;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callRecovery;
    public CheckpointManager(ExecutionGraph g, Configuration conf, Database db){
        this.isCommitted=new ArrayDeque<>();
        this.callSnapshot=new ConcurrentHashMap<>();
        this.callRecovery=new ConcurrentHashMap<>();
        this.lock=new Object();
        this.conf=conf;
        this.g=g;
        this.db=db;
        this.close=false;
        if(OsUtils.isMac()){
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("checkpointTestPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }else {
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("checkpointPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }
        this.callSnapshot_ini();
        this.callRecovery_ini();
    }

    public void initialize(boolean needRecovery) throws IOException {
        final Path parent = Current_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        checkpointFile =localFS.pathToFile(Current_Path);
        if(!needRecovery){
            LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(checkpointFile);
            DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
            Date date = new Date();
            SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
            dataOutputStream.writeUTF("System begin at "+dateFormat.format(date));
            dataOutputStream.close();
            localDataOutputStream.close();
        }
    }
    public boolean spoutRegister(long checkpointId){
        this.isCommitted.add(checkpointId);
        LOG.debug("Spout register the checkpoint with the checkpointId= "+checkpointId);
        return true;
    }
    public void boltRegister(int executorId,FaultToleranceConstants.FaultToleranceStatus status){
        if(callSnapshot.containsKey(executorId)){
            callSnapshot.put(executorId, status);
        }else{
            callRecovery.put(executorId,status);
        }
        LOG.debug("executor("+executorId+")"+" register the "+status);
    }
    private void callSnapshot_ini(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
                this.callSnapshot.put(e.getExecutorID(),NULL);
            }
        }
    }
    private void callRecovery_ini(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(!e.isLeafNode()&&!e.op.IsStateful()){
                this.callRecovery.put(e.getExecutorID(),NULL);
            }
        }
    }
    private boolean not_all_registerSnapshot(){
        return callSnapshot.containsValue(NULL);
    }
    private void execute() throws Exception {
        while (running){
            synchronized (lock){
                while(not_all_registerSnapshot()&&!close){
                    lock.wait();
                }
                if(close){
                    return;
                }
                if(enable_measure){
                    MeasureTools.FTM_receive_all_Ack(System.nanoTime());
                }
                if(callSnapshot.containsValue(Recovery)){
                    if(enable_measure){
                        MeasureTools.startRecovery(System.nanoTime());
                    }
                    LOG.debug("CheckpointManager received all register and start recovery");
                    SnapshotResult lastSnapshotResult=getLastCommitSnapshotResult(checkpointFile);
                    //this.g.topology.tableinitilizer.reloadDB(this.db.getTxnProcessingEngine().getRecoveryRangeId());
                    this.g.getSpout().recoveryInput(lastSnapshotResult.getCheckpointId());
                    MeasureTools.startReloadDB(System.nanoTime());
                    this.db.reloadStateFromSnapshot(lastSnapshotResult);
                    MeasureTools.finishReloadDB(System.nanoTime());
                    this.db.getTxnProcessingEngine().isTransactionAbort=false;
                    LOG.debug("Reload state complete!");
                    synchronized (lock){
                        while (callRecovery.containsValue(NULL)){
                            lock.wait();
                        }
                    }
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    this.isCommitted.clear();
                    notifyAllComplete();
                    LOG.debug("Recovery complete!");
                    lock.notifyAll();
                }else if(callSnapshot.containsValue(Undo)){
                    LOG.debug("CheckpointManager received all register and start undo");
                    this.db.undoFromWAL();
                    LOG.info("Undo log complete!");
                    this.db.getTxnProcessingEngine().isTransactionAbort=false;
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    notifyAllComplete();
                    lock.notifyAll();
                } else if(callSnapshot.containsValue(Persist)){
                    if(enable_measure){
                        MeasureTools.startPersist(System.nanoTime());
                    }
                    LOG.debug("CheckpointManager received all register and start snapshot");
                    if(enable_parallel){
                        this.snapshotResult=this.db.parallelSnapshot(isCommitted.poll(),00000L);
                    }else{
                        RunnableFuture<SnapshotResult> snapshotResult =this.db.snapshot(isCommitted.poll(),00000L);
                        this.snapshotResult=snapshotResult.get();
                    }
                    if(enable_measure){
                        MeasureTools.finishPersist(System.nanoTime());
                        MeasureTools.setSnapshotFileSize(this.snapshotResult.getSnapshotResults().keySet());
                    }
                    commitCurrentLog();
                    notifySnapshotComplete();
                    lock.notifyAll();
                }
            }
        }
    }
    public boolean commitCurrentLog() throws IOException, InterruptedException {
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(checkpointFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        byte[] result= Serialize.serializeObject(this.snapshotResult);
        int len=result.length;
        dataOutputStream.writeInt(len);
        dataOutputStream.write(result);
        dataOutputStream.close();
        LOG.debug("CheckpointManager commit the checkpoint to the current.log");
        return true;
    }
    public void notifySnapshotComplete() throws Exception {
        for(int id:callSnapshot.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        MeasureTools.FTM_finish_Ack(System.nanoTime());
        this.callSnapshot_ini();
    }
    public void notifyAllComplete() throws Exception {
        for(int id:callSnapshot.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callSnapshot_ini();
        for(int id:callRecovery.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callRecovery_ini();
    }
    public Object getLock(){
        return lock;
    }
    public void close(){
        this.close=true;
    }
    @Override
    public void run() {
        try{
            execute();
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                localFS.delete(Current_Path.getParent(),true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("CheckpointManager stops");
        }
    }
}
