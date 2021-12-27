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
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastGlobalLSN;

public class LoggerManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(LoggerManager.class);
    public boolean running=true;
    private Path Current_Path;
    private FileSystem localFS;
    private File walFile;
    private ExecutionGraph g;
    private Database db;
    private Configuration conf;
    private Object lock;
    private LogResult logResult;
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
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("WALTestPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }else {
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("WALPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }
        this.callLog_ini();
        this.callRecovery_ini();
    }
    public void initialize(boolean needRecovery) throws IOException {
        final Path parent = Current_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        walFile =localFS.pathToFile(Current_Path);
        if(!needRecovery){
            LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(walFile);
            DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
            Date date = new Date();
            SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
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
                    LOG.debug("LoggerManager received all register and start recovery");
                    if(enable_parallel){
                        this.g.topology.tableinitilizer.reloadDB(this.db.getTxnProcessingEngine().getRecoveryRangeId());
                        long theLastLSN=getLastGlobalLSN(walFile);
                        this.db.recoveryFromWAL(theLastLSN);
                        this.db.undoFromWAL();
                        this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    }else{
                        this.g.topology.tableinitilizer.reloadDB(this.db.getTxnProcessingEngine().getRecoveryRangeId());
                        long theLastLSN=this.db.recoveryFromWAL(-1);
                        this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                    }
                    notifyLogComplete();
                    lock.notifyAll();
                } else if (callLog.containsValue(Persist)){
                    if(enable_measure){
                        MeasureTools.startPersist(System.nanoTime());
                    }
                    LOG.debug("LoggerManager received all register and start commit log");
                    commitLog();
                    notifyLogComplete();
                    lock.notifyAll();
                    if(enable_measure){
                        MeasureTools.finishPersist(System.nanoTime());
                        MeasureTools.setWalFileSize(Current_Path.getParent());
                    }
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
    private boolean commitLog() throws IOException, ExecutionException, InterruptedException {
        long LSN=isCommitted.poll();
        RunnableFuture<LogResult> commitLog=this.db.commitLog(LSN, 00000L);
        if(commitLog!=null){
            commitLog.get();
        }
        commitGlobalLSN(LSN);
        LOG.debug("Update log commit!");
        return true;
    }
    private boolean commitGlobalLSN(long globalLSN) throws IOException, InterruptedException {
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(walFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        dataOutputStream.writeLong(globalLSN);
        dataOutputStream.close();
        LOG.debug("LoggerManager commit the globalLSN to the current.log");
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
                localFS.delete(Current_Path.getParent(),true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("WALManager stops");
        }
    }
}
