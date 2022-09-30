package streamprocess.components.operators.base.transaction;

import engine.Exception.DatabaseException;
import engine.transaction.impl.TxnManagerTStream;
import org.slf4j.Logger;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.operators.api.TransactionalBolt;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.components.topology.TopologyContext;
import streamprocess.controller.output.Epoch.EpochInfo;
import streamprocess.controller.output.InFlightLog.MultiStreamInFlightLog;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.FaultToleranceConstants;
import streamprocess.faulttolerance.clr.CausalService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public abstract class TransactionalBoltTStream extends TransactionalBolt {
    private static final long serialVersionUID = 3266583495485725150L;
    public int partition_delta;
    public EpochInfo epochInfo;
    //<DownStreamId,causalService>
    protected HashMap<Integer,CausalService> causalService = new HashMap<>();
    protected long recoveryId = -1;
    protected List<Integer> recoveryPartitionIds = new ArrayList<>();
    protected boolean isSnapshot;
    protected long markerId = 0;
    protected MultiStreamInFlightLog multiStreamInFlightLog;
    protected int firstDownTask;
    public TransactionalBoltTStream(Logger log,int fid){
        super(log,fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(),this.context.getThisComponentId(),thread_Id,NUM_SEGMENTS,this.context.getThisComponent().getNumTasks());
        partition_delta=(int) Math.ceil(NUM_ITEMS / (double) PARTITION_NUM);//NUM_ITEMS / partition_num;
        if(enable_recovery_dependency){
            this.epochInfo = new EpochInfo(0L,this.executor.getExecutorID());
        }
        if (enable_upstreamBackup) {
            multiStreamInFlightLog = new MultiStreamInFlightLog(this.executor.operator);
            for (String streamId : this.executor.operator.get_childrenStream()) {
                Map<TopologyComponent, Grouping> children = this.executor.operator.getChildrenOfStream(streamId);
                for (TopologyComponent child:children.keySet()){
                    this.firstDownTask = child.getExecutorIDList().get(0);
                }
            }
        }
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector){
        loadDB(context.getThisTaskId()-context.getThisComponent().getExecutorList().get(0).getExecutorID(),context.getThisTaskId(),context.getGraph());
    }
    //used in the T-Stream_CC
    protected abstract void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException;
    protected void REQUEST_POST() throws InterruptedException{};//implement in the application
    protected void REQUEST_CORE() throws InterruptedException{};//implement in the application
    /**
     * To register persist when there is no transaction abort
     */
    protected void AsyncRegisterPersist(){
        this.lock = this.FTM.getLock();
        synchronized (lock){
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
            this.isCommit = false;
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
            this.isCommit = false;
        }
    }
    /**
     * To register recovery when there is a failure(wal)
     * @throws InterruptedException
     */
    protected void SyncRegisterRecovery() throws InterruptedException {
        this.lock = this.FTM.getLock();
        synchronized (lock){
            this.FTM.boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Recovery);
            lock.notifyAll();
        }
        synchronized (lock){
            while(!isCommit){
                LOG.debug("Wait for the database to recovery");
                lock.wait();
            }
            this.isCommit = false;
        }
        for (TopologyComponent child:this.executor.getChildren_keySet()) {
            for (ExecutionNode e:child.getExecutorList()) {
                if (enable_determinants_log) {
                    for (int lostPartitionId : this.recoveryPartitionIds) {
                        if (!enable_key_based || this.executor.operator.getExecutorIDList().get(lostPartitionId) == this.executor.getExecutorID()) {
                            this.causalService.put(e.getExecutorID(), e.askCausalService().get(lostPartitionId));
                        }
                    }
                }
            }
        }
        LOG.info("Align offset is  " + this.recoveryId);
    }
    public int getPartitionId(String key){
        Integer _key = Integer.valueOf(key);
        return _key / partition_delta;
    }
    public void updateRecoveryDependency(int[] key, boolean isModify){
        int[] partitionId = new int[key.length];
        for (int i = 0; i < key.length; i++){
            partitionId[i] = getPartitionId(String.valueOf(key[i]));
        }
        this.epochInfo.addDependency(partitionId,isModify);
    }
    public void updateRecoveryDependency(String[] key, boolean isModify){
        int[] partitionId = new int[key.length];
        for (int i = 0; i < key.length; i++){
            partitionId[i] = getPartitionId(key[i]);
        }
        this.epochInfo.addDependency(partitionId,isModify);
    }

    @Override
    public void cleanEpoch(long offset) {
        if (enable_upstreamBackup) {
            this.multiStreamInFlightLog.cleanEpoch(offset, DEFAULT_STREAM_ID);
        }
    }

    @Override
    public void setRecoveryId(long alignMarkerId){
        this.recoveryId = alignMarkerId;
    }
}
