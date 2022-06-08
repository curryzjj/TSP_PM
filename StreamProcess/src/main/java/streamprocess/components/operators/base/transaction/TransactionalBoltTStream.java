package streamprocess.components.operators.base.transaction;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import engine.transaction.impl.TxnManagerTStream;
import org.slf4j.Logger;
import streamprocess.components.operators.api.TransactionalBolt;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.components.topology.TopologyContext;
import streamprocess.controller.output.Determinant.OutsideDeterminant;
import streamprocess.controller.output.Epoch.EpochInfo;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.FaultToleranceConstants;
import streamprocess.faulttolerance.clr.CausalService;
import streamprocess.faulttolerance.clr.ComputationLogic;
import streamprocess.faulttolerance.clr.ComputationTask;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.*;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public abstract class TransactionalBoltTStream extends TransactionalBolt {
    public int partition_delta;
    public List<ComputationTask> tasks;
    public List<ComputationLogic> computationLogics;
    public EpochInfo epochInfo;
    protected HashMap<Integer,CausalService> causalService;
    protected long recoveryId = -1;
    protected boolean isSnapshot;
    protected long markerId = 0;
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
            this.epochInfo = new EpochInfo(0L,this.executor.getExecutorID());
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
    public void BUFFER_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        for (Queue<Tuple> tuples : bufferedTuples.values()) {
            if (tuples.size() !=0) {
                boolean isMarker = false;
                while (!isMarker) {
                    Tuple tuple = tuples.poll();
                    if (tuple != null) {
                        execute(tuple);
                        if (tuple.isMarker()) {
                            isMarker =true;
                        }
                    } else {
                        isMarker = true;
                    }
                }
            }
        }
    }
    /**
     * To register persist when there is no transaction abort
     */
    protected void AsyncRegisterPersist(){
        this.lock=this.FTM.getLock();
        synchronized (lock){
            if (enable_measure){
                MeasureTools.bolt_register_Ack(this.thread_Id,System.nanoTime());
            }
            if (isSnapshot) {
                this.FTM.boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Snapshot);
                isSnapshot = false;
            } else
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
        for (TopologyComponent child:this.executor.getChildren_keySet()) {
            for (ExecutionNode e:child.getExecutorList()) {
                if (enable_determinants_log) {
                    this.causalService.put(e.getExecutorID(),e.askCausalService().get(this.executor.getExecutorID()));
                    this.recoveryId = this.causalService.get(e.getExecutorID()).currentMarkerId;
                } else if (enable_recovery_dependency){
                    this.recoveryId = e.ackRecoveryDependency().currentMarkId;
                }
            }
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
    public void updateRecoveryDependency(int[] key, boolean isWrite){
        int[] partitionId = new int[key.length];
        for (int i = 0; i < key.length; i++){
            partitionId[i] = getPartitionId(String.valueOf(key[i]));
        }
        this.epochInfo.addDependency(partitionId,isWrite);
    }
}
