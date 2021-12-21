package streamprocess.faulttolerance.clr;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import org.checkerframework.checker.units.qual.C;
import org.jctools.queues.MpscArrayQueue;
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

import static UserApplications.CONTROL.enable_measure;
import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;

public class CLRManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(CLRManager.class);
    public  boolean running=true;
    private FileSystem localFS;
    private EventManager eventManager;
    private Path Current_Path;
    private File clrFile;
    private ExecutionGraph g;
    private Database db;
    private Configuration conf;
    private Object lock;
    private boolean close;
    private Queue<EventsTask> isCommitted;
    private Map<Integer,Queue> computationTasksQueue;
    private EventsTask currentEventsTask;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callEvent;
    public CLRManager(ExecutionGraph g,Configuration conf,Database db){
        this.isCommitted=new ArrayDeque<>();
        this.callEvent=new ConcurrentHashMap<>();
        this.lock=new Object();
        this.conf=conf;
        this.g=g;
        this.db=db;
        this.close=false;
        this.eventManager=new EventManager();
        this.computationTasksQueue=new HashMap<>();
        if(OsUtils.isMac()){
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("CLRTestPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }else {
            this.Current_Path=new Path(System.getProperty("user.home").concat(conf.getString("CLRPath")),"CURRENT");
            this.localFS=new LocalFileSystem();
        }
        this.callEvent_ini();
    }
    @Override
    public void initialize(boolean needRecovery) throws IOException {
        final Path parent = Current_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        clrFile =localFS.pathToFile(Current_Path);
        if(!needRecovery){
            LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(clrFile);
            DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
            Date date = new Date();
            SimpleDateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd :hh:mm:ss");
            dataOutputStream.writeUTF("System begin at "+dateFormat.format(date));
            dataOutputStream.close();
            localDataOutputStream.close();
        }
    }
    private void execute() throws InterruptedException, IOException {
        while (running){
            synchronized (lock){
                while (not_all_registerEvent()&&!close){
                    lock.wait();
                }
                if(close){
                    return;
                }
                if(callEvent.containsValue(Persist)){
                    LOG.debug("CLRManager received all register and start commit event task");
                    commitEvent();
                    notifyAllComplete();
                    lock.notifyAll();
                }else if(callEvent.containsValue(Recovery)){
                    LOG.debug("CLRManager received all register and start recovery");
                    this.g.topology.tableinitilizer.reloadDB(this.db.getTxnProcessingEngine().getRecoveryRangeId());
                    this.recoveryComputationTask();
                    notifyAllComplete();
                    lock.notifyAll();
                    this.eventManager.loadComputationTasks(this.db.getTxnProcessingEngine().getRecoveryRangeId(),Current_Path.getParent(),computationTasksQueue);
                    this.db.getTxnProcessingEngine().getRecoveryRangeId().clear();
                }
            }
        }
    }
    private void notifyAllComplete() {
        for(int id:callEvent.keySet()){
            g.getExecutionNode(id).ackCommit();
        }
        this.callEvent_ini();
        if(isCommitted.size()!=0){
            this.currentEventsTask=isCommitted.poll();
        }
        if (enable_measure){
            MeasureTools.FTM_finish_Ack(System.nanoTime());
        }
    }
    @Override
    public boolean spoutRegister(long eventTaskId) {
        EventsTask eventsTask=new EventsTask(eventTaskId);
        isCommitted.add(eventsTask);
        if(currentEventsTask==null){
            currentEventsTask=isCommitted.poll();
        }
        LOG.debug("Spout register the wal with the eventTaskId "+eventTaskId);
        return true;
    }

    @Override
    public void boltRegister(int executorId, FaultToleranceConstants.FaultToleranceStatus status) {
        callEvent.put(executorId,status);
        LOG.debug("executor("+executorId+")"+" register the "+status);
    }
    private void callEvent_ini(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
                this.callEvent.put(e.getExecutorID(),NULL);
            }
        }
    }
    private boolean not_all_registerEvent(){
        return callEvent.containsValue(NULL);
    }

    @Override
    public void commitComputationTasks(List<ComputationTask> tasks) {
        currentEventsTask.addComputationTask(tasks);
    }

    @Override
    public Queue getComputationTasks(int executorId) {
        return this.computationTasksQueue.get(executorId);
    }

    private void commitEvent() throws IOException, InterruptedException {
        eventManager.persistEventsTask(this.Current_Path.getParent(),currentEventsTask);
    }
    private void recoveryComputationTask(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
               this.computationTasksQueue.put(e.getExecutorID(),new MpscArrayQueue(1000000));
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
        } catch (InterruptedException | IOException e) {
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
