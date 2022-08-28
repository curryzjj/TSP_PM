package streamprocess.faulttolerance.logger;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.SOURCE_CONTROL;
import engine.Database;
import engine.log.LogResult;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;

import static System.Constants.SSD_Path;
import static UserApplications.CONTROL.*;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastCommitSnapshotResult;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastGlobalLSN;

public class LoggerManager extends FTManager {
    private final Logger LOG = LoggerFactory.getLogger(LoggerManager.class);
    public boolean running = true;
    private FileSystem localFS;
    //WAL location
    private Path WAL_Path;
    private File walFile;
    private String LogFolderName;
    //Snapshot location
    private Path Snapshot_Path;
    private File snapshotFile;
    private ExecutionGraph g;
    private Database db;
    private Configuration conf;
    private final Object lock;
    private LogResult logResult;
    private HashMap<Long, SnapshotResult> snapshotResults = new HashMap<>();
    private boolean close;
    private Queue<Long> SnapshotOffset;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callLog;
    private ConcurrentHashMap<Integer,FaultToleranceConstants.FaultToleranceStatus> callRecovery;
    public LoggerManager(ExecutionGraph g,Configuration conf,Database db){
        this.SnapshotOffset = new ArrayDeque<>();
        this.callLog = new ConcurrentHashMap<>();
        this.callRecovery = new ConcurrentHashMap<>();
        this.lock=new Object();
        this.conf=conf;
        this.g=g;
        this.db=db;
        this.close=false;
        if(OsUtils.isMac()){
            this.WAL_Path = new Path(System.getProperty("user.home").concat(conf.getString("WALTestPath")),"CURRENT");
            this.Snapshot_Path = new Path(System.getProperty("user.home").concat(conf.getString("checkpointTestPath")),"CURRENT");
        }else {
            this.WAL_Path = new Path(SSD_Path.concat(conf.getString("WALPath")),"CURRENT");
            this.Snapshot_Path = new Path(SSD_Path.concat(conf.getString("checkpointPath")),"CURRENT");
        }
        this.localFS = new LocalFileSystem();
        this.callLog_ini();
        this.callRecovery_ini();
    }
    public void initialize(boolean needRecovery) throws IOException {
        Path parent = WAL_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        parent = Snapshot_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        walFile = localFS.pathToFile(WAL_Path);
        snapshotFile = localFS.pathToFile(Snapshot_Path);
        this.LogFolderName = UUID.randomUUID().toString();
        this.db.setWalPath(LogFolderName);
        if(!needRecovery){
            LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(walFile);
            DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
            Date date = new Date();
            SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
            dataOutputStream.writeUTF("System begin at "+dateFormat.format(date));
            localDataOutputStream=new LocalDataOutputStream(snapshotFile);
            dataOutputStream=new DataOutputStream(localDataOutputStream);
            dataOutputStream.writeUTF("System begin at "+dateFormat.format(date));
            dataOutputStream.close();
            localDataOutputStream.close();
        }
    }
    public void boltRegister(int executorId,FaultToleranceConstants.FaultToleranceStatus status){
        if(callLog.containsKey(executorId)){
            callLog.put(executorId,status);
        }else{
            callRecovery.put(executorId,status);
        }
        LOG.debug("executor("+executorId+")"+" register the "+status);
    }
    public boolean spoutRegister(long globalLSN){
        SnapshotOffset.add(globalLSN);
        LOG.info("Spout register the wal with the globalLSN= " + globalLSN);
        return true;
    }

    @Override
    public boolean sinkRegister(long id) throws IOException, InterruptedException {
        commitGlobalLSN(id);
        if (snapshotResults.containsKey(id)) {
            this.commitCurrentLog(id);
        }
        return true;
    }

    private void callLog_ini() {
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
                this.callLog.put(e.getExecutorID(),NULL);
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
    private boolean not_all_registerLog(){
        return callLog.containsValue(NULL);
    }
    private void execute() throws Exception{
        while (running){
            synchronized (lock){
                while(not_all_registerLog()&&!close){
                    lock.wait();
                }
                if(close){
                    return;
                }
                if(callLog.containsValue(Undo)){
                    LOG.info("LoggerManager received all register and start Undo");
                    this.db.undoFromWAL();
                    LOG.info("Undo log complete!");
                    this.db.getTxnProcessingEngine().isTransactionAbort.compareAndSet(true, false);
                    notifyLogComplete();
                    lock.notifyAll();
                }else if(callLog.containsValue(Recovery)){
                    LOG.info("LoggerManager received all register and start recovery");
                    failureFlag.compareAndSet(true, false);
                    failureTimes ++;
                    SnapshotResult lastSnapshotResult = getLastCommitSnapshotResult(snapshotFile);
                    long theLastLSN = getLastGlobalLSN(walFile);
                    MeasureTools.State_load_begin(System.nanoTime());
                    this.g.getSpout().recoveryInput(lastSnapshotResult.getCheckpointId(),new ArrayList<>(), theLastLSN);
                    this.g.getSink().clean_status();
                    LOG.info("Reload database from lastSnapshot");
                    this.db.reloadStateFromSnapshot(lastSnapshotResult);
                    LOG.info("Align offset is  " + theLastLSN);
                    LOG.info("Reload state at " + lastSnapshotResult.getCheckpointId() + " complete!");
                    MeasureTools.State_load_finish(System.nanoTime());
                    MeasureTools.RedoLog_time_begin(System.nanoTime());
                    LOG.info("Replay committed transactions");
                    if(enable_parallel){
                        this.db.recoveryFromWAL(theLastLSN);
                    }else{
                        theLastLSN = this.db.recoveryFromWAL(-1);
                    }
                    MeasureTools.RedoLog_time_finish(System.nanoTime());
                    LOG.info("Replay committed transactions complete!");
                    synchronized (lock){
                        while (callRecovery.containsValue(NULL)){
                            lock.wait();
                        }
                    }
                    this.SnapshotOffset = new ArrayDeque<>();
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    this.db.getTxnProcessingEngine().isTransactionAbort.compareAndSet(true, false);
                    this.db.getTxnProcessingEngine().cleanAllOperations();
                    SOURCE_CONTROL.getInstance().config(PARTITION_NUM);
                    notifyAllComplete();
                    lock.notifyAll();
                } else if (callLog.containsValue(Persist)){
                    LOG.info("LoggerManager received all register and start commit log");
                    MeasureTools.startWAL(System.nanoTime());
                    commitLog();
                    MeasureTools.finishWAL(System.nanoTime());
                    MeasureTools.setWalFileSize(new Path(WAL_Path.getParent().toString().concat("/").concat(LogFolderName)));
                    notifyLogComplete();
                    lock.notifyAll();
                } else if (callLog.containsValue(Snapshot)) {
                    LOG.info("LoggerManager received all register and start snapshot");
                    MeasureTools.startWAL(System.nanoTime());
                    long offset = commitLog();
                    MeasureTools.finishWAL(System.nanoTime());
                    MeasureTools.setWalFileSize(new Path(WAL_Path.getParent().toString().concat("/").concat(LogFolderName)));
                    SnapshotResult snapshotResult;
                    MeasureTools.startSnapshot(System.nanoTime());
                    if(enable_parallel){
                        snapshotResult = this.db.parallelSnapshot(offset,00000L);

                    }else{
                        snapshotResult = this.db.snapshot(offset,00000L).get();
                    }
                    this.snapshotResults.put(snapshotResult.getCheckpointId(), snapshotResult);
                    MeasureTools.finishSnapshot(System.nanoTime());
                    MeasureTools.setSnapshotFileSize(snapshotResult.getSnapshotPaths());
                    notifyLogComplete();
                    this.LogFolderName = UUID.randomUUID().toString();
                    this.db.setWalPath(LogFolderName);
                    lock.notifyAll();
                }
            }
        }
    }

    private void notifyLogComplete() {
        for(int id:callLog.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callLog_ini();
    }
    public void notifyAllComplete() throws Exception {
        for(int id:callLog.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callLog_ini();
        for(int id:callRecovery.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callRecovery_ini();
    }
    private long commitLog() throws IOException, ExecutionException, InterruptedException {
        long LSN = this.SnapshotOffset.poll();
        RunnableFuture<LogResult> commitLog = this.db.commitLog(LSN, 00000L);
        if(commitLog != null){
            commitLog.get();
        }
        LOG.info("Update log commit!");
        return LSN;
    }
    private boolean commitGlobalLSN(long globalLSN) throws IOException, InterruptedException {
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(walFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        dataOutputStream.writeLong(globalLSN);
        dataOutputStream.close();
        LOG.info("LoggerManager commit the globalLSN "+ globalLSN +" to the current.log");
        return true;
    }
    public boolean commitCurrentLog(long snapshotOffset) throws IOException, InterruptedException {
        LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(snapshotFile);
        DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
        byte[] result= Serialize.serializeObject(this.snapshotResults.get(snapshotOffset));
        int len = result.length;
        dataOutputStream.writeInt(len);
        dataOutputStream.write(result);
        dataOutputStream.close();
        LOG.info("LoggerManager commit the checkpoint " + snapshotOffset +" to the current.log");
        return true;
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
                /* Delete the current and wal log file */
                localFS.delete(WAL_Path.getParent(),true);
                localFS.delete(Snapshot_Path.getParent(),true);

            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("WALManager stops");
            LOG.info("Failure Time : " + failureTimes);
        }
    }
}
