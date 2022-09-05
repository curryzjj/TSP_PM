package engine.transaction.impl;

import UserApplications.SOURCE_CONTROL;
import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.AbstractStorageManager;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.TxnContext;
import engine.transaction.TxnManagerDedicated;
import engine.transaction.TxnProcessingEngine;
import engine.transaction.common.Operation;
import engine.transaction.common.OperationChain;
import engine.transaction.function.Condition;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

public class ConventionalTxnManager extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(ConventionalTxnManager.class);
    TxnProcessingEngine instance;

    public ConventionalTxnManager(AbstractStorageManager storageManager, String thisComponentId, int thread_Id, int NUM_SEGMENTS, int num_tasks) {
        super(storageManager,thisComponentId,thread_Id,num_tasks);
        instance=TxnProcessingEngine.getInstance();
    }

    @Override
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord tableRecord, SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType) {
        long bid=txn_context.getBID();
        operation_chain_construction_modify_read(tableRecord,srcTable,bid,accessType,record_ref,function,txn_context);
        return true;
    }
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord s_record, SchemaRecordRef record_ref, Function function,
                                              TableRecord[] condition_source, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {
        long bid = txn_context.getBID();
        operation_chain_construction_modify_read(srcTable, bid, accessType, s_record, record_ref, function, condition_source, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.

        return true;

    }

    protected boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, SchemaRecordRef record_ref, double[] enqueue_time, MetaTypes.AccessType accessType) {
        long bid = txn_context.getBID();
        operation_chain_construction_read_only(t_record, primary_key, table_name, bid, accessType, record_ref, txn_context);
        return true;//it should be always success.
    }

    @Override
    protected boolean Asy_WriteRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, String primary_key, List<DataBox> value, double[] enqueue_time, MetaTypes.AccessType access_type) {
        long bid = txn_context.getBID();
        operation_chain_construction_write_only(t_record, primary_key, table_name, bid, access_type, value, txn_context);
        return true;//it should be always success.
    }
    @Override
    protected boolean Asy_WriteRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, long value, int column_id, MetaTypes.AccessType access_type) {
        long bid = txn_context.getBID();
        operation_chain_construction_write_only(t_record, primary_key, table_name, bid, access_type, value, column_id, txn_context);
        return true;//it should be always success.
    }
    @Override
    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord t_record, TableRecord d_record, Function function, MetaTypes.AccessType accessType, int column_id) {
        long bid = txn_context.getBID();
        operation_chain_construction_modify_only(t_record, srcTable, bid, accessType, d_record, function, txn_context, column_id);//TODO: this is for sure READ_WRITE... think about how to further optimize.
        return true;
    }
    @Override
    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord s_record, TableRecord d_record, Function function, TableRecord[] condition_source, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {
        long bid = txn_context.getBID();
        operation_chain_construction_modify_only(srcTable, bid, accessType, s_record, d_record, function, condition_source, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.
        return true;
    }

    private void operation_chain_construction_modify_only(TableRecord s_record, String srcTable, long bid, MetaTypes.AccessType accessType, TableRecord d_record, Function function, TxnContext txn_context, int column_id) {
        String primaryKey = d_record.record_.GetPrimaryKey();
        ConcurrentHashMap<String, OperationChain<Operation>> holder = instance.getHolder(srcTable).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new OperationChain(srcTable, primaryKey, getPartitionId(primaryKey)));
        Operation operation = new Operation(srcTable, s_record, d_record, bid, accessType, function, txn_context, column_id);
        holder.get(primaryKey).add(operation);
        txn_context.addBrotherOperations(operation, holder.get(primaryKey));
    }
    private void operation_chain_construction_modify_only(String table_name, long bid, MetaTypes.AccessType accessType, TableRecord s_record, TableRecord d_record, Function function, TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {
        String primaryKey = d_record.record_.GetPrimaryKey();
        ConcurrentHashMap<String, OperationChain<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new OperationChain(table_name, primaryKey, getPartitionId(primaryKey)));
        Operation operation = new Operation(table_name, s_record, d_record, null, bid, accessType, function, condition_records, condition, txn_context, success);
        holder.get(primaryKey).add(operation);
        txn_context.addBrotherOperations(operation, holder.get(primaryKey));
    }

    //operation_chain_construction
    private void operation_chain_construction_modify_read(TableRecord tableRecord, String srcTable, long bid, MetaTypes.AccessType accessType, SchemaRecordRef record_ref, Function function, TxnContext txn_context) {
        String primaryKey=tableRecord.record_.GetPrimaryKey();
        ConcurrentHashMap<String, OperationChain<Operation>> holder = instance.getHolder(srcTable).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey,new OperationChain<>(srcTable,primaryKey, getPartitionId(primaryKey)));
        Operation operation = new Operation(srcTable, txn_context, bid, accessType, tableRecord, record_ref, function);
        holder.get(primaryKey).add(operation);
        txn_context.addBrotherOperations(operation, holder.get(primaryKey));
    }
    private void operation_chain_construction_modify_read(String table_name, long bid, MetaTypes.AccessType accessType, TableRecord d_record, SchemaRecordRef record_ref, Function function
            , TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {
        String primaryKey = d_record.record_.GetPrimaryKey();
        ConcurrentHashMap<String, OperationChain<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new OperationChain(table_name, primaryKey, getPartitionId(primaryKey)));
        Operation operation = new Operation(table_name, d_record, d_record, record_ref, bid, accessType, function, condition_records, condition, txn_context, success);
        holder.get(primaryKey).add(operation);
        txn_context.addBrotherOperations(operation, holder.get(primaryKey));
    }

    public void operation_chain_construction_read_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, SchemaRecordRef record_ref, TxnContext txn_context) {
        ConcurrentHashMap<String, OperationChain<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new OperationChain(table_name, primaryKey, getPartitionId(primaryKey)));
        Operation operation = new Operation(table_name, txn_context, bid, accessType, record, record_ref);
        holder.get(primaryKey).add(operation);
        txn_context.addBrotherOperations(operation, holder.get(primaryKey));
    }
    private void operation_chain_construction_write_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, List<DataBox> value, TxnContext txn_context) {
        ConcurrentHashMap<String, OperationChain<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new OperationChain(table_name, primaryKey, getPartitionId(primaryKey)));
        Operation operation = new Operation(table_name, txn_context, bid, accessType, record, value);
        holder.get(primaryKey).add(operation);
        txn_context.addBrotherOperations(operation, holder.get(primaryKey));
    }
    private void operation_chain_construction_write_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, long value, int column_id, TxnContext txn_context) {
        ConcurrentHashMap<String, OperationChain<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new OperationChain(table_name, primaryKey, getPartitionId(primaryKey)));
        Operation operation = new Operation(table_name, txn_context, bid, accessType, record, value, column_id);
        holder.get(primaryKey).add(operation);
        txn_context.addBrotherOperations(operation, holder.get(primaryKey));
    }
    @Override
    public int start_evaluate(int thread_id, long mark_ID) throws InterruptedException, BrokenBarrierException, IOException, DatabaseException {
        /* Pay attention to concurrency control */
        if (instance.start_evaluation(thread_id,mark_ID)) {
            if (instance.isTransactionAbort.get()) {
                SOURCE_CONTROL.getInstance().Wait_End(thread_id);
                instance.isTransactionAbort.compareAndSet(true, false);
                return 1;
            } else {
                this.instance.cleanOperations(thread_id);
                //implement the SOURCE_CONTROL sync for all threads to come to this line.
                return 0;
            }
        } else {
            /* Some failures have happened */
            return 2;
        }
    }
}
