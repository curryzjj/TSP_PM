package streamprocess.faulttolerance.checkpoint;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.SOURCE_CONTROL;
import engine.Database;
import engine.shapshot.SnapshotResult;
import engine.table.datatype.serialize.Serialize;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;

import static System.Constants.SSD_Path;
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
    private ConcurrentHashMap<Long,SnapshotResult> snapshotResults = new ConcurrentHashMap<>();
    private boolean close;
    private Queue<Long> SnapshotOffset;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callSnapshot;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callRecovery;
    public CheckpointManager(ExecutionGraph g, Configuration conf, Database db){
        this.SnapshotOffset = new ArrayDeque<>();
        this.callSnapshot = new ConcurrentHashMap<>();
        this.callRecovery = new ConcurrentHashMap<>();
        this.lock = new Object();
        this.conf = conf;
        this.g=g;
        this.db=db;
        this.close=false;
        if(OsUtils.isMac()){
            this.Current_Path = new Path(System.getProperty("user.home").concat(conf.getString("checkpointTestPath")),"CURRENT");
            this.localFS = new LocalFileSystem();
        }else {
            this.Current_Path = new Path(SSD_Path.concat(conf.getString("checkpointPath")),"CURRENT");
            this.localFS = new LocalFileSystem();
        }
        this.callSnapshot_ini();
        this.callRecovery_ini();
    }

    public void initialize(boolean needRecovery) throws IOException {
        final Path parent = Current_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        checkpointFile = localFS.pathToFile(Current_Path);
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
        this.SnapshotOffset.add(checkpointId);
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

    @Override
    public boolean sinkRegister(long id) throws IOException {
        return this.commitCurrentLog(id);
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
                if(callSnapshot.containsValue(Recovery)){
                    LOG.info("CheckpointManager received all register and start recovery");
                    failureFlag.compareAndSet(true, false);
                    failureTimes ++;
                    SnapshotResult lastSnapshotResult = getLastCommitSnapshotResult(checkpointFile);
                    this.g.getSpout().recoveryInput(lastSnapshotResult.getCheckpointId(), new ArrayList<>(), lastSnapshotResult.getCheckpointId());
                    MeasureTools.State_load_begin(System.nanoTime());
                    this.db.reloadStateFromSnapshot(lastSnapshotResult);
                    MeasureTools.State_load_finish(System.nanoTime());
                    this.db.getTxnProcessingEngine().isTransactionAbort.compareAndSet(true, false);
                    LOG.info("Reload state at " + lastSnapshotResult.getCheckpointId() + " complete!");
                    synchronized (lock){
                        while (callRecovery.containsValue(NULL)){
                            lock.wait();
                        }
                    }
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    this.db.getTxnProcessingEngine().cleanAllOperations();
                    SOURCE_CONTROL.getInstance().config(PARTITION_NUM);
                    this.SnapshotOffset.clear();
                    this.g.getSink().clean_status();
                    notifyAllComplete();
                    LOG.info("Recovery complete!");
                    lock.notifyAll();
                }else if(callSnapshot.containsValue(Undo)){
                    LOG.debug("CheckpointManager received all register and start undo");
                    this.db.undoFromWAL();
                    LOG.info("Undo log complete!");
                    this.db.getTxnProcessingEngine().isTransactionAbort.compareAndSet(true, false);
                    notifyBoltComplete();
                    lock.notifyAll();
                } else if(callSnapshot.containsValue(Snapshot)){
                    LOG.info("CheckpointManager received all register and start snapshot");
                    SnapshotResult snapshotResult;
                    MeasureTools.startSnapshot(System.nanoTime());
                    if(enable_parallel){
                        snapshotResult = this.db.parallelSnapshot(SnapshotOffset.poll(),00000L);
                    }else{
                        RunnableFuture<SnapshotResult> getSnapshotResult = this.db.snapshot(SnapshotOffset.poll(),00000L);
                        snapshotResult = getSnapshotResult.get();
                    }
                    MeasureTools.finishSnapshot(System.nanoTime());
                    MeasureTools.setSnapshotFileSize(snapshotResult.getSnapshotPaths());
                    this.snapshotResults.put(snapshotResult.getCheckpointId(),snapshotResult);
                    notifyBoltComplete();
                    lock.notifyAll();
                }
            }
        }
    }
    public boolean commitCurrentLog(long id) throws IOException {
        LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(checkpointFile);
        DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
        byte[] result = Serialize.serializeObject(this.snapshotResults.get(id));
        int len = result.length;
        dataOutputStream.writeInt(len);
        dataOutputStream.write(result);
        dataOutputStream.close();
        LOG.info("CheckpointManager commit the checkpoint to the current.log");
        return true;
    }
    public void notifyBoltComplete() throws Exception {
        for(int id:callSnapshot.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
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
            LOG.info("Failure Time : " + failureTimes);
        }
    }
}
