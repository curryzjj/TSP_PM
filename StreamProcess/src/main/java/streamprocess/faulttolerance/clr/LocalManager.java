package streamprocess.faulttolerance.clr;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.SOURCE_CONTROL;
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
import static UserApplications.CONTROL.PARTITION_NUM;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.NULL;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastCommitSnapshotResult;

public class LocalManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(LocalManager.class);
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
    protected Queue<Long> commitOffset;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callFaultTolerance;
    private ConcurrentHashMap<Integer,FaultToleranceConstants.FaultToleranceStatus> callRecovery;
    public LocalManager(ExecutionGraph g,Configuration conf,Database db){
        this.callFaultTolerance = new ConcurrentHashMap<>();
        this.callRecovery = new ConcurrentHashMap<>();
        this.lock = new Object();
        this.conf = conf;
        this.g=g;
        this.db=db;
        this.close=false;
        this.SnapshotOffset = new ArrayDeque<>();
        this.commitOffset = new ArrayDeque<>();
        if(OsUtils.isMac()){
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("checkpointTestPath")),"CURRENT");
        }else {
            this.Current_Path=new Path(SSD_Path.concat(conf.getString("checkpointPath")),"CURRENT");
        }
        this.localFS=new LocalFileSystem();
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
                    failureFlag.compareAndSet(true, false);
                    failureTimes ++;
                    List<Integer> recoveryIds;
                    long alignOffset;
                    if (enable_determinants_log) {
                        recoveryIds = this.db.getTxnProcessingEngine().getRecoveryRangeId();
                        ConcurrentHashMap<Integer,CausalService> causalService = this.g.getSink().askCausalService();
                        alignOffset = causalService.get(recoveryIds.get(0)).currentMarkerId;
                    } else {
                        RecoveryDependency recoveryDependency = this.g.getSink().ackRecoveryDependency();
                        alignOffset = recoveryDependency.currentMarkId;
                        recoveryIds = this.db.getTxnProcessingEngine().getRecoveryRangeId();
                    }
                    LOG.info("Recovery partitions are" + recoveryIds.toString() + "Align offset is  " + alignOffset);
                    SnapshotResult lastSnapshotResult = getLastCommitSnapshotResult(SnapshotFile);
                    this.g.getSpout().recoveryInput(lastSnapshotResult.getCheckpointId(), recoveryIds, alignOffset);
                    MeasureTools.State_load_begin(System.nanoTime());
                    this.db.recoveryFromTargetSnapshot(lastSnapshotResult, recoveryIds);
                    MeasureTools.State_load_finish(System.nanoTime());
                    LOG.info("Reload state at " + lastSnapshotResult.getCheckpointId() + " complete!");
                    synchronized (lock){
                        while (callRecovery.containsValue(NULL)){
                            lock.wait();
                        }
                    }
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    this.db.getTxnProcessingEngine().cleanAllOperations();
                    this.db.getTxnProcessingEngine().isTransactionAbort.compareAndSet(true, false);
                    SOURCE_CONTROL.getInstance().config(PARTITION_NUM);
                    this.SnapshotOffset.clear();
                    this.g.getSink().clean_status();
                    notifyAllComplete(alignOffset);
                    lock.notifyAll();
                } else if(callFaultTolerance.containsValue(Snapshot)) {
                    LOG.info("CLRManager received all bolt register and start snapshot");
                    MeasureTools.startSnapshot(System.nanoTime());
                    SnapshotResult snapshotResult = this.db.parallelSnapshot(this.SnapshotOffset.poll(),00000L);
                    this.snapshotResults.put(snapshotResult.getCheckpointId(), snapshotResult);
                    MeasureTools.finishSnapshot(System.nanoTime());
                    MeasureTools.setSnapshotFileSize(snapshotResult.getSnapshotPaths());
                    this.snapshotResults.put(snapshotResult.getCheckpointId(),snapshotResult);
                    if (commitOffset.contains(snapshotResult.getCheckpointId())) {
                        this.commitCurrentLog(commitOffset.poll());
                    }
                    notifyBoltComplete();
                }
            }
        }
    }
    public boolean commitCurrentLog(long checkpointId) throws IOException {
        if (this.snapshotResults.get(checkpointId) != null){
            LocalDataOutputStream localDataOutputStream= new LocalDataOutputStream(SnapshotFile);
            DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
            byte[] result= Serialize.serializeObject(this.snapshotResults.get(checkpointId));
            int len=result.length;
            dataOutputStream.writeInt(len);
            dataOutputStream.write(result);
            dataOutputStream.close();
            LOG.info("CLRManager commit the checkpoint to the current.log at " + checkpointId);
            g.getSpout().ackCommit(checkpointId);
            g.getSink().ackCommit(checkpointId);
            for (int eId : this.callFaultTolerance.keySet()) {
                g.getExecutionNode(eId).ackCommit(checkpointId);
            }
            db.cleanUndoLog(checkpointId);
        } else {
            this.commitOffset.add(checkpointId);
        }
        return true;
    }
    public void notifyBoltComplete() throws Exception {
        this.callFaultTolerance_ini();
    }
    private void notifyAllComplete(long alignMakerId) {
        for(int id: callFaultTolerance.keySet()){
            g.getExecutionNode(id).ackCommit(true, alignMakerId);
        }
        this.callFaultTolerance_ini();
        for(int id:callRecovery.keySet()){
            g.getExecutionNode(id).ackCommit(true, alignMakerId);
        }
        this.callRecovery_ini();
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
            LOG.info("Failure Time : " + failureTimes);
        }
    }
}
