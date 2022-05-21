package streamprocess.faulttolerance.logger;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.log.LogResult;
import engine.shapshot.SnapshotResult;
import engine.table.datatype.serialize.Serialize;
import org.apache.commons.lang.RandomStringUtils;
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

import static UserApplications.CONTROL.enable_measure;
import static UserApplications.CONTROL.enable_parallel;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastCommitSnapshotResult;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastGlobalLSN;

public class LoggerManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(LoggerManager.class);
    public boolean running=true;
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
    private Object lock;
    private LogResult logResult;
    private SnapshotResult snapshotResult;
    private boolean close;
    private Queue<Long> isCommitted;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callLog;
    private ConcurrentHashMap<Integer,FaultToleranceConstants.FaultToleranceStatus> callRecovery;
    public LoggerManager(ExecutionGraph g,Configuration conf,Database db){
        this.isCommitted=new ArrayDeque<>();
        this.callLog=new ConcurrentHashMap<>();
        this.callRecovery=new ConcurrentHashMap<>();
        this.lock=new Object();
        this.conf=conf;
        this.g=g;
        this.db=db;
        this.close=false;
        if(OsUtils.isMac()){
            this.WAL_Path =new Path(System.getProperty("user.home").concat(conf.getString("WALTestPath")),"CURRENT");
            this.Snapshot_Path =new Path(System.getProperty("user.home").concat(conf.getString("checkpointTestPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }else {
            this.WAL_Path =new Path(System.getProperty("user.home").concat(conf.getString("WALPath")),"CURRENT");
            this.Snapshot_Path =new Path(System.getProperty("user.home").concat(conf.getString("checkpointPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }
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
        walFile =localFS.pathToFile(WAL_Path);
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
        isCommitted.add(globalLSN);
        LOG.debug("Spout register the wal with the globalLSN= "+globalLSN);
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
                if(enable_measure){
                    MeasureTools.FTM_receive_all_Ack(System.nanoTime());
                }
                if(callLog.containsValue(Undo)){
                    if(enable_measure){
                        MeasureTools.startUndoTransaction(System.nanoTime());
                    }
                    LOG.debug("LoggerManager received all register and start Undo");
                    this.db.undoFromWAL();
                    LOG.debug("Undo log complete!");
                    this.db.getTxnProcessingEngine().isTransactionAbort=false;
                    notifyLogComplete();
                    lock.notifyAll();
                    if(enable_measure){
                        MeasureTools.finishUndoTransaction(System.nanoTime());
                    }
                }else if(callLog.containsValue(Recovery)){
                    if(enable_measure){
                        MeasureTools.startRecovery(System.nanoTime());
                    }
                    LOG.debug("LoggerManager received all register and start recovery");
                    LOG.debug("Reload database from lastSnapshot");
                    MeasureTools.startReloadDB(System.nanoTime());
                    SnapshotResult lastSnapshotResult = getLastCommitSnapshotResult(snapshotFile);
                    if (lastSnapshotResult == null){
                        this.g.topology.tableinitilizer.reloadDB(this.db.getTxnProcessingEngine().getRecoveryRangeId());
                    } else {
                        this.db.reloadStateFromSnapshot(lastSnapshotResult);
                    }
                    MeasureTools.finishReloadDB(System.nanoTime());
                    LOG.debug("Replay committed transactions");
                    long theLastLSN=getLastGlobalLSN(walFile);
                    if(enable_parallel){
                        this.db.recoveryFromWAL(theLastLSN);
                    }else{
                        theLastLSN = this.db.recoveryFromWAL(-1);
                    }
                    LOG.debug("Replay committed transactions complete!");
                    this.g.getSpout().recoveryInput(theLastLSN,null);
                    this.isCommitted.clear();
                    this.db.undoFromWAL();
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    synchronized (lock){
                        while (callRecovery.containsValue(NULL)){
                            lock.wait();
                        }
                    }
                    notifyAllComplete();
                    lock.notifyAll();
                } else if (callLog.containsValue(Persist)){
                    if(enable_measure){
                        MeasureTools.startPersist(System.nanoTime());
                    }
                    LOG.info("LoggerManager received all register and start commit log");
                    commitLog();
                    if(enable_measure){
                        MeasureTools.finishPersist(System.nanoTime());
                        MeasureTools.setWalFileSize(WAL_Path.getParent());
                    }
                    notifyLogComplete();
                    lock.notifyAll();
                } else if (callLog.containsValue(Snapshot)) {
                    if(enable_measure){
                        MeasureTools.startPersist(System.nanoTime());
                    }
                    LOG.info("LoggerManager received all register and start commit log");
                    long offset = commitLog();
                    if(enable_measure){
                        MeasureTools.finishPersist(System.nanoTime());
                        MeasureTools.setWalFileSize(WAL_Path.getParent());
                    }
                    if(enable_parallel){
                        this.snapshotResult=this.db.parallelSnapshot(offset,00000L);
                    }else{
                        RunnableFuture<SnapshotResult> snapshotResult =this.db.snapshot(offset,00000L);
                        this.snapshotResult=snapshotResult.get();
                    }
                    commitCurrentLog();
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
        if (enable_measure){
            MeasureTools.FTM_finish_Ack(System.nanoTime());
        }
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
        long LSN=isCommitted.poll();
        RunnableFuture<LogResult> commitLog=this.db.commitLog(LSN, 00000L);
        this.g.getSpout().ackCommit(LSN);
        if(commitLog!=null){
            commitLog.get();
        }
        commitGlobalLSN(LSN);
        LOG.info("Update log commit!");
        return LSN;
    }
    private boolean commitGlobalLSN(long globalLSN) throws IOException, InterruptedException {
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(walFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        dataOutputStream.writeLong(globalLSN);
        dataOutputStream.close();
        LOG.debug("LoggerManager commit the globalLSN to the current.log");
        return true;
    }
    public boolean commitCurrentLog() throws IOException, InterruptedException {
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(snapshotFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        byte[] result= Serialize.serializeObject(this.snapshotResult);
        int len=result.length;
        dataOutputStream.writeInt(len);
        dataOutputStream.write(result);
        dataOutputStream.close();
        LOG.debug("CheckpointManager commit the checkpoint to the current.log");
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
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("WALManager stops");
        }
    }
}
