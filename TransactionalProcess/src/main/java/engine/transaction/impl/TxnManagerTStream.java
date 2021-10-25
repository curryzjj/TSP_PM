package engine.transaction.impl;

import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.StorageManager;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.TxnContext;
import engine.transaction.TxnManager;
import engine.transaction.TxnManagerDedicated;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TxnManagerTStream extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerTStream.class);
    //TxnProcessingEngine instance;

    public TxnManagerTStream(StorageManager storageManager) {
        super(storageManager);
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
    }
}
