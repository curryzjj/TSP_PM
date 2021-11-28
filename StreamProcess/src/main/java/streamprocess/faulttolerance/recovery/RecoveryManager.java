package streamprocess.faulttolerance.recovery;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.shapshot.SnapshotResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.faulttolerance.FTManager;
import streamprocess.faulttolerance.FaultToleranceConstants;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import static UserApplications.CONTROL.*;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastCommitSnapshotResult;
import static streamprocess.faulttolerance.recovery.RecoveryHelperProvider.getLastGlobalLSN;

public class RecoveryManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(RecoveryManager.class);
    public boolean running=true;
    private boolean close;
    private Path Current_Path;
    private FileSystem localFS;
    private File recoveryFile;
    private ExecutionGraph g;
    private Database db;
    private Configuration conf;
    private Object lock;
    private boolean needRecovery;
    private boolean completeRecovery;
    private int inputRecoveryOffset;
    private long startTime;
    private long finishTime;
    private SnapshotResult lastSnapshotResult;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callRecovery;
    public RecoveryManager(ExecutionGraph g,Configuration conf,Database db){
        this.callRecovery=new ConcurrentHashMap<>();
        this.lock=new Object();
        this.conf=conf;
        this.g=g;
        this.db=db;
        this.close=false;
        this.completeRecovery=false;
        this.callRecovery_ini();
        String faultTolerance="";
        if(enable_snapshot){
            faultTolerance="checkpoint";
        }else if(enable_wal){
            faultTolerance="WAL";
        }
        if(OsUtils.isMac()){
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString(faultTolerance+"TestPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }else {
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString(faultTolerance+"Path")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }
    }
    private void callRecovery_ini(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.getExecutorID()!=-2){
                this.callRecovery.put(e.getExecutorID(),NULL);
            }
        }
    }
    @Override
    public void initialize() throws IOException {
        recoveryFile=localFS.pathToFile(Current_Path);
        if(recoveryFile.exists()){
            this.needRecovery=true;
        }else{
            this.needRecovery=false;
        }
    }

    public boolean spoutRegister(int executorId) {
        if(needRecovery){
            this.callRecovery.put(executorId, Recovery);
            LOG.info("executor("+executorId+")"+" register the recovery");
        }
        return needRecovery;
    }

    @Override
    public void boltRegister(int executorId,FaultToleranceConstants.FaultToleranceStatus status) {
        callRecovery.put(executorId, status);
        LOG.info("executor("+executorId+")"+" register the recovery");
    }

    @Override
    public Object getLock() {
        return lock;
    }

    public boolean needRecovery(){
        return needRecovery;
    }
    private void notifyRecoveryComplete() {
        for(int id:callRecovery.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callRecovery_ini();
    }
    private boolean not_registerRecovery(){
        return callRecovery.containsValue(NULL);
    }
    @Override
    public void close() {
        this.close=true;
    }
    private void execute() throws Exception {
        while (running){
            if(needRecovery){
                synchronized (lock){
                    while(not_registerRecovery()){
                        lock.wait();
                    }
                    LOG.info("RecoveryManager received all register and start recovery");
                    this.startTime=System.nanoTime();
                    recovery();
                    this.finishTime=System.nanoTime();
                    this.completeRecovery=true;
                    this.needRecovery=false;
                    notifyRecoveryComplete();
                    LOG.info("Recovery takes "+(finishTime - startTime) / 1E6 + " ms"+" and complete!");
                    lock.notifyAll();
                }
            }else{
                if(completeRecovery==false){
                    LOG.info("No need to recovery for the first execution");
                }
                this.running=false;
            }
        }
    }

    private void recovery() throws IOException, ClassNotFoundException, DatabaseException, InterruptedException {
        if(enable_snapshot){
            this.lastSnapshotResult=getLastCommitSnapshotResult(recoveryFile);
            this.db.recoveryFromSnapshot(lastSnapshotResult);
            this.g.getSpout().recoveryInput(lastSnapshotResult.getCheckpointId());
        }else if (enable_wal){
            if(enable_parallel){
                long theLastLSN=getLastGlobalLSN(recoveryFile);
                this.db.recoveryFromWAL(theLastLSN);
                this.g.getSpout().recoveryInput(theLastLSN);
            }else{
                long theLastLSN=this.db.recoveryFromWAL(-1);
                this.g.getSpout().recoveryInput(theLastLSN);
            }
        }
    }

    @Override
    public void run() {
        try{
            execute();
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            LOG.info("RecoveryManager stops");
        }
    }
}
