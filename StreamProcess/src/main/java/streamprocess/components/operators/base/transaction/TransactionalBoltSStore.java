package streamprocess.components.operators.base.transaction;

import applications.events.GlobalSorter;
import applications.events.TxnEvent;
import engine.Exception.DatabaseException;
import engine.transaction.TxnManager;
import engine.transaction.impl.TxnManagerSStore;
import org.slf4j.Logger;
import streamprocess.components.operators.api.TransactionalBolt;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.FaultToleranceConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.*;

public abstract class TransactionalBoltSStore extends TransactionalBolt {
    private static final long serialVersionUID = -3629093039373797683L;
    public int partition_delta;
    protected long recoveryId = -1;
    public List<TxnEvent> EventsHolder = new ArrayList<>();
    public TransactionalBoltSStore(Logger log, int fid) {
        super(log, fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(),this.context.getThisComponentId(),thread_Id, this.context.getThisComponent().getNumTasks());
        partition_delta = NUM_ITEMS / PARTITION_NUM;//NUM_ITEMS / partition_num;
    }
    @Override
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException;
    public abstract void Sort_Lock(int thread_Id) ;
    public abstract void PostLAL_Process(TxnEvent event) throws DatabaseException;
    public abstract void POST_PROCESS(TxnEvent txnEvent) throws InterruptedException;
    public abstract void LAL(TxnEvent event) throws DatabaseException;
    public abstract boolean checkAbort(TxnEvent txnEvent);

    public void LA_LOCK_Reentrant(TxnManager txnManager, long[] bid_array, int[] partition_Indexes, long _bid){
        for (int _pid : partition_Indexes) {
            txnManager.getOrderLock(_pid).blocking_wait(bid_array[_pid], _bid);
        }
    }
    public static void LA_UNLOCK_Reentrant(TxnManager txnManager, int[] partition_Indexes) {
        for (int _pid : partition_Indexes) {
            txnManager.getOrderLock(_pid).advance();
        }
    }
    public void LAL_PROCESS(TxnEvent event) throws DatabaseException{
        LA_LOCK_Reentrant(transactionManager, event.getBid_array(), event.partition_indexes, event.getBid());
        LAL(event);
        LA_UNLOCK_Reentrant(transactionManager, event.partition_indexes);
    }
    public void LA_RESETALL(TxnManager txnManager, int tthread) {
        txnManager.getOrderLock(tthread).reset();
    }
    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnEvent event = (TxnEvent) in.getValue(0);
        GlobalSorter.addEvent(event);
        EventsHolder.add(event);
    }
    public void syncSnapshot(){
        try {
            this.FTM.takeSnapshot(checkpointId,this.thread_Id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
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
        LOG.info("Align offset is  " + this.recoveryId);
    }
    @Override
    public void setRecoveryId(long alignMarkerId){
        this.recoveryId = alignMarkerId;
    }
    public int getPartitionId(String key){
        Integer _key = Integer.valueOf(key);
        return _key / partition_delta;
    }
}
