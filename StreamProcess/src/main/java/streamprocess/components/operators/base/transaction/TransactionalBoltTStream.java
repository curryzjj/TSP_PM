package streamprocess.components.operators.base.transaction;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import engine.transaction.impl.TxnManagerTStream;
import org.slf4j.Logger;
import streamprocess.components.operators.api.TransactionalBolt;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.FaultToleranceConstants;
import streamprocess.faulttolerance.clr.ComputationLogic;
import streamprocess.faulttolerance.clr.ComputationTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.*;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public abstract class TransactionalBoltTStream extends TransactionalBolt {
    public int partition_delta;
    public List<ComputationTask> tasks;
    public List<ComputationLogic> computationLogics;
    public TransactionalBoltTStream(Logger log,int fid){
        super(log,fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager=new TxnManagerTStream(db.getStorageManager(),this.context.getThisComponentId(),thread_Id,NUM_SEGMENTS,this.context.getThisComponent().getNumTasks());
        partition_delta=(int) Math.ceil(NUM_ITEMS / (double) partition_num);//NUM_ITEMS / partition_num;
        if(enable_clr){
            this.tasks=new ArrayList<>();
            this.computationLogics=new ArrayList<>();
        }
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector){
        loadDB(context.getThisTaskId()-context.getThisComponent().getExecutorList().get(0).getExecutorID(),context.getThisTaskId(),context.getGraph());
    }
    //used in the T-Stream_CC
    protected abstract void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException;
    protected abstract boolean TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException;
    protected abstract boolean TXN_PROCESS()throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException;
    protected void REQUEST_POST() throws InterruptedException{};//implement in the application
    protected void REQUEST_REQUEST_CORE() throws InterruptedException{};//implement in the application
    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        if(status.isMarkerArrived(in.getSourceTask())){
            PRE_EXECUTE(in);
        }else{
            PRE_TXN_PROCESS(in);
        }
    }
    /**
     * To register persist when there is no transaction abort
     */
    protected void AsyncRegisterPersist(){
        this.lock=this.FTM.getLock();
        if(enable_clr){
            this.FTM.commitComputationTasks(tasks);
            //this.FTM.commitComputationLogics(computationLogics);
        }
        synchronized (lock){
            if (enable_measure){
                MeasureTools.bolt_register_Ack(this.thread_Id,System.nanoTime());
            }
            this.FTM.boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Persist);
            lock.notifyAll();
        }
    }

    /**
     * Wait for the log to commit then emit the result to output
     * @throws InterruptedException
     */
    protected void SyncCommitLog() throws InterruptedException {
        synchronized (lock){
            while(!isCommit){
                //wait for log to commit
                LOG.debug("Wait for the log to commit");
                lock.wait();
            }
            if (enable_measure){
                MeasureTools.bolt_receive_ack_time(this.thread_Id,System.nanoTime());
            }
            this.isCommit =false;
        }
    }

    /**
     * To register undo when there is transaction abort
     */
    protected void SyncRegisterUndo() throws InterruptedException {
        this.lock=this.FTM.getLock();
        synchronized (lock){
            this.FTM.boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Undo);
            lock.notifyAll();
        }
        synchronized (lock){
            while(!isCommit){
                LOG.debug("Wait for the database to undo");
                lock.wait();
            }
            this.isCommit =false;
        }
    }
    /**
     * To register recovery when there is a failure(wal)
     * @throws InterruptedException
     */
    protected void SyncRegisterRecovery() throws InterruptedException {
        this.lock=this.FTM.getLock();
        synchronized (lock){
            this.FTM.boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Recovery);
            lock.notifyAll();
        }
        synchronized (lock){
            while(!isCommit){
                LOG.debug("Wait for the database to recovery");
                lock.wait();
            }
            this.isCommit =false;
        }
    }
    /**
     * To register recovery when there is a failure(snapshot)
     * @throws InterruptedException
     */
    protected void registerRecovery() throws InterruptedException {
        this.lock=this.getContext().getRM().getLock();
        this.getContext().getRM().boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Recovery);
        synchronized (lock){
            while (!isCommit){
                LOG.debug(this.executor.getOP_full()+" is waiting for the Recovery");
                lock.wait();
            }
        }
        isCommit=false;
    }
    public int getPartitionId(String key){
        Integer _key = Integer.valueOf(key);
        return _key / partition_delta;
    }
}
