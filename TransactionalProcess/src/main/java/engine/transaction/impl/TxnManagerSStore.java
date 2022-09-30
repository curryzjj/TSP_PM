package engine.transaction.impl;

import System.util.SpinLock;
import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.AbstractStorageManager;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.TxnContext;
import engine.transaction.TxnManagerDedicated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Lock.PartitionedOrderLock;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

import static UserApplications.CONTROL.PARTITION_NUM;
import static UserApplications.CONTROL.enable_states_partition;

public class TxnManagerSStore extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerSStore.class);
    public final PartitionedOrderLock orderLock;
    public final ConcurrentHashMap<Integer, SpinLock> spinLocks = new ConcurrentHashMap<>();
    public TxnManagerSStore(AbstractStorageManager storageManager, String thisComponentId, int thread_Id, int num_tasks) {
        super(storageManager, thisComponentId, thread_Id, num_tasks);
        this.orderLock = PartitionedOrderLock.getInstance();
        this.orderLock.initilize(PARTITION_NUM);
        for (int i = 0; i < PARTITION_NUM; i++) {
            spinLocks.put(i, new SpinLock());
        }
    }

    public PartitionedOrderLock.LOCK getOrderLock(int p_id) {
        return orderLock.get(p_id);
    }

    @Override
    public int start_evaluate(int taskId, long mark_ID) throws InterruptedException, BrokenBarrierException, IOException, DatabaseException {
        return 0;
    }

    @Override
    public boolean lock_ahead(String table_name, String primary_key, MetaTypes.AccessType accessType) throws DatabaseException {
        String tableName="";
        if(enable_states_partition){
            tableName = table_name+"_"+ getPartitionId(primary_key);
        }else{
            tableName = table_name;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(primary_key);
        if (t_record != null) {
            this.spinLocks.get(getPartitionId(primary_key)).lock();
            return true;
        } else {
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String primary_key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException {
        String tableName= "";
        if(enable_states_partition){
            tableName = table_name+"_"+ getPartitionId(primary_key);
        }else{
            tableName = table_name;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(primary_key);
        if (t_record != null) {
            record_ref.setRecord(t_record.record_);//Note that, locking scheme allows directly modifying on original table d_record.
            return true;
        } else {
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public void CommitTransaction(List<String> keys) {
        for (String primary_key : keys) {
            this.spinLocks.get(getPartitionId(primary_key)).unlock();
        }
    }
}
