package streamprocess.faulttolerance.logger;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.log.LogResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.faulttolerance.FTManager;
import streamprocess.faulttolerance.checkpoint.CheckpointConstants;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;

import static streamprocess.faulttolerance.checkpoint.CheckpointConstants.CheckpointStatus.NULL;
import static streamprocess.faulttolerance.checkpoint.CheckpointConstants.CheckpointStatus.Register;

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
    //TODO:add the log result
    private LogResult logResult;
    private boolean close;
    private long currentGlobalLSN;
    private ConcurrentHashMap<Long,Boolean> isCommitted;
    private ConcurrentHashMap<Integer, CheckpointConstants.CheckpointStatus> callLog;
    public LoggerManager(ExecutionGraph g,Configuration conf,Database db){
        this.isCommitted=new ConcurrentHashMap<>();
        this.callLog=new ConcurrentHashMap<>();
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
    }
    public void initialize() throws IOException {
        final Path parent = Current_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        walFile =localFS.pathToFile(Current_Path);
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(walFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        Date date = new Date();
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
        dataOutputStream.writeUTF("System begin at "+dateFormat.format(date));
        dataOutputStream.close();
        localDataOutputStream.close();
    }
    public void boltRegister(int executorId){
        callLog.put(executorId,Register);
        LOG.info("executor("+executorId+")"+" register the log commit");
    }
    public boolean spoutRegister(long globalLSN){
        if(isCommitted.size()>10){
            return false;
        }
        isCommitted.put(globalLSN,false);
        return true;
    }
    private void callLog_ini() {
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
                this.callLog.put(e.getExecutorID(),NULL);
            }
        }
    }
    private boolean not_all_registerLog(){
        return callLog.containsValue(NULL);
    }
    private  void execute() throws Exception{
        while (running){
            synchronized (lock){
                while(not_all_registerLog()&&!close){
                    lock.wait();
                }
                if(close){
                    return;
                }
                LOG.info("LoggerManager received all register and start commit log");
                commitLog();
                notifyLogComplete();
                lock.notifyAll();
            }
        }
    }

    private void notifyLogComplete() {
        for(int id:callLog.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callLog_ini();
    }

    private boolean commitLog() throws IOException, ExecutionException, InterruptedException {
        RunnableFuture<LogResult> commitLog=this.db.commitLog(this.currentGlobalLSN,00000L);
        commitLog.get();
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
            LOG.info("WALManager stops");
        }
    }
}
