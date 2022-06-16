package streamprocess.faulttolerance.clr;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.Exception.DatabaseException;
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

import static System.Constants.SSD_Path;
import static UserApplications.CONTROL.*;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastCommitSnapshotResult;

public class CLRManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(CLRManager.class);
    public  boolean running=true;
    private FileSystem localFS;

    private Path Current_Path;
    private File SnapshotFile;
    private HashMap<Long, SnapshotResult> snapshotResults = new HashMap<>();
    private ExecutionGraph g;
    private Database db;
    private Configuration conf;
    private Object lock;
    private boolean close;
    private Queue<Long> SnapshotOffset;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callFaultTolerance;
    private ConcurrentHashMap<Integer,FaultToleranceConstants.FaultToleranceStatus> callRecovery;
    public CLRManager(ExecutionGraph g,Configuration conf,Database db){
        this.callFaultTolerance = new ConcurrentHashMap<>();
        this.callRecovery = new ConcurrentHashMap<>();
        this.lock = new Object();
        this.conf = conf;
        this.g=g;
        this.db=db;
        this.close=false;
        this.SnapshotOffset = new ArrayDeque<>();

        if(OsUtils.isMac()){
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("checkpointTestPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }else {
            this.Current_Path=new Path(SSD_Path.concat(conf.getString("checkpointPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }
        this.callFaultTolerance_ini();
        this.callRecovery_ini();
    }
    @Override
    public void initialize(boolean needRecovery) throws IOException {
        final Path parent = Current_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        SnapshotFile = localFS.pathToFile(Current_Path);
        if(!needRecovery){
            LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(SnapshotFile);
            DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
            Date date = new Date();
            SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
            dataOutputStream.writeUTF("System begin at "+dateFormat.format(date));
            dataOutputStream.close();
            localDataOutputStream.close();
        }
    }
    @Override
    public boolean spoutRegister(long checkpintId) {
        this.SnapshotOffset.add(checkpintId);
        LOG.debug("Spout register the checkpoint with the checkpointId = " + checkpintId);
        return true;
    }
    @Override
    public void boltRegister(int executorId, FaultToleranceConstants.FaultToleranceStatus status) {
        if (callFaultTolerance.containsKey(executorId)) {
             callFaultTolerance.put(executorId,status);
        } else  {
            callRecovery.put(executorId,status);
        }
        LOG.debug("executor("+executorId+")"+" register the "+status);
    }
    @Override
    public boolean sinkRegister(long id) throws IOException {
        return this.commitCurrentLog(id);
    }

    private void execute() throws Exception {
        while (running){
            synchronized (lock){
                while (not_all_register()&&!close){
                    lock.wait();
                }
                if(close){
                    return;
                }
                if(callFaultTolerance.containsValue(Recovery)){
                    LOG.info("CLRManager received all bolt register and start recovery");
                    SnapshotResult lastSnapshotResult = getLastCommitSnapshotResult(SnapshotFile);
                    List<Integer> recoveryIds;
                    long alignOffset;
                    if (enable_determinants_log) {
                        recoveryIds = this.db.getTxnProcessingEngine().getRecoveryRangeId();
                        ConcurrentHashMap<Integer,CausalService> causalService = this.g.getSink().askCausalService();
                        alignOffset = causalService.get(recoveryIds.get(0)).currentMarkerId;
                    } else {
                        RecoveryDependency recoveryDependency = this.g.getSink().ackRecoveryDependency();
                        alignOffset = recoveryDependency.currentMarkId;
                        recoveryIds = recoveryDependency.getDependencyByPatitionId(this.db.getTxnProcessingEngine().getRecoveryRangeId());
                    }
                    //undo to align offset
                    if (enable_align_wait) {
                        this.db.undoFromWALToTargetOffset(recoveryIds,alignOffset);
                    }
                    this.g.getSpout().recoveryInput(lastSnapshotResult.getCheckpointId(),recoveryIds, alignOffset);
                    this.db.recoveryFromTargetSnapshot(lastSnapshotResult,recoveryIds);
                    this.db.getTxnProcessingEngine().isTransactionAbort=false;
                    LOG.info("Reload state at " + lastSnapshotResult.getCheckpointId() + " complete!");
                    synchronized (lock){
                        while (callRecovery.containsValue(NULL)){
                            lock.wait();
                        }
                    }
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    notifyAllComplete();
                    lock.notifyAll();
                } else if(callFaultTolerance.containsValue(Snapshot)) {
                    LOG.info("CLRManager received all bolt register and start snpshot");
                    MeasureTools.startSnapshot(System.nanoTime());
                    SnapshotResult snapshotResult = this.db.parallelSnapshot(this.SnapshotOffset.poll(),00000L);
                    this.snapshotResults.put(snapshotResult.getCheckpointId(),snapshotResult);
                    MeasureTools.finishSnapshot(System.nanoTime());
                    notifySnapshotComplete(snapshotResult.getCheckpointId());
                    lock.notifyAll();
                } else if(callFaultTolerance.containsValue(Undo)){
                    LOG.info("CLRManager received all register and start undo");
                    this.db.undoFromWAL();
                    LOG.info("Undo log complete!");
                    this.db.getTxnProcessingEngine().isTransactionAbort=false;
                    notifyAllComplete();
                    lock.notifyAll();
                }
            }
        }
    }
    public boolean commitCurrentLog(long checkpointId) throws IOException {
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(SnapshotFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        byte[] result= Serialize.serializeObject(this.snapshotResults.get(checkpointId));
        int len=result.length;
        dataOutputStream.writeInt(len);
        dataOutputStream.write(result);
        dataOutputStream.close();
        LOG.debug("CLRManager commit the checkpoint to the current.log");
        g.getSpout().ackCommit(checkpointId);
        g.getSink().ackCommit(checkpointId);
        for (int eId : this.callFaultTolerance.keySet()) {
            g.getExecutionNode(eId).ackCommit(checkpointId);
        }
        db.cleanUndoLog(checkpointId);
        return true;
    }
    public void notifySnapshotComplete(long offset) throws Exception {
        for(int id: callFaultTolerance.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callFaultTolerance_ini();
    }
    private void notifyAllComplete() {
        for(int id: callFaultTolerance.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callFaultTolerance_ini();
        for(int id:callRecovery.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callRecovery_ini();
        if (enable_measure){
            MeasureTools.FTM_finish_Ack(System.nanoTime());
        }
    }
    private boolean not_all_register(){
        return callFaultTolerance.containsValue(NULL);
    }



    private void callFaultTolerance_ini(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
                this.callFaultTolerance.put(e.getExecutorID(),NULL);
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
    @Override
    public Object getLock() {
        return lock;
    }

    @Override
    public void close() {
        this.close=true;
    }

    @Override
    public void run() {
        try{
            execute();
        } catch (InterruptedException | IOException | DatabaseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                /* Delete the current and wal log file */
                localFS.delete(Current_Path.getParent(),true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("CLRManager stops");
        }
    }
}
