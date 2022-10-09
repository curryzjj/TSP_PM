package streamprocess.faulttolerance;

import System.util.Configuration;
import engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static streamprocess.faulttolerance.FaultToleranceConstants.FaultToleranceStatus.*;

public class BaseManager extends FTManager{
    private final Logger LOG= LoggerFactory.getLogger(BaseManager.class);
    public boolean running=true;
    private ExecutionGraph g;
    private Database db;
    private Object lock;
    private boolean close;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callAbort;
    public BaseManager(ExecutionGraph g, Configuration conf, Database db){
        this.callAbort = new ConcurrentHashMap<>();
        this.lock = new Object();
        this.g = g;
        this.db = db;
        this.close = false;
        this.callAbort_ini();
    }

    public void initialize(boolean needRecovery) throws IOException {

    }
    public void boltRegister(int executorId,FaultToleranceConstants.FaultToleranceStatus status){
        callAbort.put(executorId, status);
        LOG.debug("executor("+executorId+")"+" register the "+status);
    }


    private void callAbort_ini(){
        for (ExecutionNode e:g.getExecutionNodeArrayList()){
            if(e.op.IsStateful()){
                this.callAbort.put(e.getExecutorID(),NULL);
            }
        }
    }
    private boolean not_all_register(){
        return callAbort.containsValue(NULL);
    }
    private void execute() throws Exception {
        while (running){
            synchronized (lock){
                while(not_all_register()&&!close){
                    lock.wait();
                }
                if(close){
                    return;
                }
                if(callAbort.containsValue(Undo)){
                    LOG.info("BaseManager received all register and start undo");
                    this.db.getTxnProcessingEngine().isTransactionAbort.compareAndSet(true, false);
                    notifyBoltComplete();
                    lock.notifyAll();
                }
            }
        }
    }
    public void notifyBoltComplete() throws Exception {
        for(int id: callAbort.keySet()){
            g.getExecutionNode(id).ackCommit(false, 0L);
        }
        this.callAbort_ini();
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
            LOG.info("BaseManager stops");
        }
    }
}
