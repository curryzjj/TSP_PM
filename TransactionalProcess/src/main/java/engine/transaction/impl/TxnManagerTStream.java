package engine.transaction.impl;

import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.AbstractStorageManager;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.table.tableRecords.TableRecordRef;
import engine.transaction.TxnContext;
import engine.transaction.TxnManagerDedicated;
import engine.transaction.TxnProcessingEngine;
import engine.transaction.common.MyList;
import engine.transaction.common.Operation;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

import static UserApplications.CONTROL.*;

public class TxnManagerTStream extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerTStream.class);
    TxnProcessingEngine instance;

    public TxnManagerTStream(AbstractStorageManager storageManager, String thisComponentId, int thread_Id, int NUM_SEGMENTS, int num_tasks) {
        super(storageManager,thisComponentId,thread_Id,num_tasks);
        instance=TxnProcessingEngine.getInstance();
    }

    @Override
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord tableRecord, SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType) {
        if(this.instance.getTransactionAbort().contains(txn_context.getBID())){
            this.instance.getTransactionAbort().remove(txn_context.getBID());
            return false;
        }
        long bid=txn_context.getBID();
        operation_chain_construction_modify_read(tableRecord,srcTable,bid,accessType,record_ref,function,txn_context);
        return true;
    }

    protected boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, SchemaRecordRef record_ref, double[] enqueue_time, MetaTypes.AccessType accessType) {
        if(this.instance.getTransactionAbort().contains(txn_context.getBID())){
            this.instance.getTransactionAbort().remove(txn_context.getBID());
            return false;
        }
        long bid = txn_context.getBID();
        operation_chain_construction_read_only(t_record, primary_key, table_name, bid, accessType, record_ref, txn_context);

        return true;//it should be always success.
    }

    @Override
    protected boolean Asy_WriteRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, String primary_key, List<DataBox> value, double[] enqueue_time, MetaTypes.AccessType access_type) {
        if(this.instance.getTransactionAbort().contains(txn_context.getBID())){
            this.instance.getTransactionAbort().remove(txn_context.getBID());
            return false;
        }
        long bid = txn_context.getBID();
        operation_chain_construction_write_only(t_record, primary_key, table_name, bid, access_type, value, txn_context);
        return true;//it should be always success.
    }

    //operation_chain_construction
    private void operation_chain_construction_modify_read(TableRecord tableRecord, String srcTable, long bid, MetaTypes.AccessType accessType, SchemaRecordRef record_ref, Function function, TxnContext txn_context) {
        String primaryKey=tableRecord.record_.GetPrimaryKey();
        ConcurrentHashMap<String, MyList<Operation>> holder=instance.getHolder(srcTable).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey,new MyList<>(srcTable,primaryKey,getTaskId(primaryKey)));
        holder.get(primaryKey).add(new Operation(srcTable,txn_context,bid,accessType,tableRecord,record_ref,function));
    }

    public void operation_chain_construction_read_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, SchemaRecordRef record_ref, TxnContext txn_context) {
        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey,getTaskId(primaryKey)));
        MyList<Operation> myList = holder.get(primaryKey);
        myList.add(new Operation(table_name, txn_context, bid, accessType, record, record_ref));
    }
    private void operation_chain_construction_write_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, List<DataBox> value, TxnContext txn_context) {
        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey,getTaskId(primaryKey)));
        holder.get(primaryKey).add(new Operation(table_name, txn_context, bid, accessType, record, value));
    }

    @Override
    public int start_evaluate(int thread_id, long mark_ID) throws InterruptedException, BrokenBarrierException, IOException, DatabaseException {
        /** Pay attention to concurrency control */
        instance.start_evaluation(thread_id,mark_ID);
        if(instance.isTransactionAbort){
            return 1;
        }else if(instance.getRecoveryRangeId().size()!=0) {
            if(thread_id==0){
                if(enable_states_partition&&enable_parallel){
                    this.storageManager.cleanTable(instance.getRecoveryRangeId());
                }else{
                    this.storageManager.cleanAllTables();
                }
            }
            return 2;
        }else {
            return 0;
        }
    }
}
