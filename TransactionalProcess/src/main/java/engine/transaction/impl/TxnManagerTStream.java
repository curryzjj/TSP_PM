package engine.transaction.impl;

import engine.Meta.MetaTypes;
import engine.storage.StorageManager;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.TxnContext;
import engine.transaction.TxnManagerDedicated;
import engine.transaction.TxnProcessingEngine;
import engine.transaction.common.MyList;
import engine.transaction.common.Operation;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

public class TxnManagerTStream extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerTStream.class);
    TxnProcessingEngine instance;
    protected int delta;//range of each partition. depends on the number of op in the stage.

    public TxnManagerTStream(StorageManager storageManager) {
        super(storageManager);
    }
    private int getTaskId(String key) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }
    @Override
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord tableRecord, SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType) {
        long bid=txn_context.getBID();
        operation_chain_construction_modify_read(tableRecord,srcTable,bid,accessType,record_ref,function,txn_context);
        return true;
    }
    //operation_chain_construction
    private void operation_chain_construction_modify_read(TableRecord tableRecord, String srcTable, long bid, MetaTypes.AccessType accessType, SchemaRecordRef record_ref, Function function, TxnContext txn_context) {
        String primaryKey=tableRecord.record_.GetPrimaryKey();
        ConcurrentHashMap<String, MyList<Operation>> holder=instance.getHolder(srcTable).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey,new MyList<>(srcTable,primaryKey));
        holder.get(primaryKey).add(new Operation(srcTable,txn_context,bid,accessType,tableRecord,record_ref,function));
    }

    @Override
    public void start_evaluate(int thread_id, long mark_ID) throws InterruptedException, BrokenBarrierException {
        instance.start_evaluation(thread_id,mark_ID);
    }
}