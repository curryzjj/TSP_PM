package streamprocess.faulttolerance.checkpoint;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.shapshot.SnapshotResult;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.faulttolerance.logger.LoggerManager;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static streamprocess.faulttolerance.checkpoint.CheckpointConstants.CheckpointStatus.*;

public class CheckpointManager extends Thread {
    private final Logger LOG= LoggerFactory.getLogger(CheckpointManager.class);
    public Status status=null;
    public boolean running=true;
    private LoggerManager LM;
    private Path Current_Path;
    private FileSystem localFS;
    private File checkpointFile;
    private ExecutionGraph g;
    private Database db;
    private Configuration conf;
    private Object lock;
    private SnapshotResult snapshotResult;
    private boolean close;
    private long currentCheckpointId;
    private ConcurrentHashMap<Long,Boolean> isCommitted;
    private ConcurrentHashMap<Integer,CheckpointConstants.CheckpointStatus> callSnapshot;
    public CheckpointManager(ExecutionGraph g, Configuration conf, Database db){
        this.isCommitted=new ConcurrentHashMap<>();
        this.callSnapshot=new ConcurrentHashMap<>();
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
    }

    public void initialize() throws IOException {
        final Path parent = Current_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        checkpointFile =localFS.pathToFile(Current_Path);
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(checkpointFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        Date date = new Date();
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
        dataOutputStream.writeUTF("System begin at "+dateFormat.format(date));
        dataOutputStream.close();
        localDataOutputStream.close();
    }
    public boolean commitCurrentLog() throws IOException, InterruptedException {
        Thread.sleep(2000);
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(checkpointFile);
        ObjectOutputStream objectOutputStream=new ObjectOutputStream(localDataOutputStream);
        objectOutputStream.writeObject(snapshotResult);
        objectOutputStream.close();
        LOG.info("CheckpointManager commit the checkpoint to the current.log");
        return true;
    }
    public void notifyCheckpointComplete() throws Exception {
        for(int id:callSnapshot.keySet()){
            g.getExecutionNode(id).ackCheckpointCommit();
        }
        this.callSnapshot_ini();
        isCommitted.put(currentCheckpointId,true);
    }
    public boolean spoutRegisterSnapshot(long checkpointId){
        if(isCommitted.containsValue(false)){
            return false;
        }else {
            this.currentCheckpointId=checkpointId;
            isCommitted.put(checkpointId,false);
            LOG.info("Spout register the checkpoint with the checkpointId= "+checkpointId);
            return true;
        }
    }
    public Object getLock(){
        return lock;
    }
    public void closeCM(){
        this.close=true;
    }
    public void boltRegisterSnapshot(int executorId){
        callSnapshot.put(executorId, Register);
        LOG.info("executor("+executorId+")"+" register the checkpoint");
    }
    private void callSnapshot_ini(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
                this.callSnapshot.put(e.getExecutorID(),NULL);
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
                LOG.info("CheckpointManager received all register and start snapshot");
                SnapshotResult snapshotResult=this.db.snapshot(this.currentCheckpointId,00000L).get();
                commitCurrentLog();
                notifyCheckpointComplete();
                lock.notifyAll();
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
            LOG.info("CheckpointManager stops");
        }
    }
}
