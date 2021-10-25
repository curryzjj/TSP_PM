package engine.transaction;

import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.StorageManager;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TxnManagerDedicated implements TxnManager{
    public static final Logger LOG= LoggerFactory.getLogger(TxnManagerDedicated.class);
    protected final StorageManager storageManager;

    public TxnManagerDedicated(StorageManager storageManager){
        this.storageManager = storageManager;
    }
    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType= MetaTypes.AccessType.READ_WRITE_READ;
        TableRecord tableRecord=storageManager.getTable(srcTable).SelectKeyRecord(key);
        if(tableRecord!=null){
            return Asy_ModifyRecord_ReadCC(txn_context,srcTable,tableRecord,record_ref,function,accessType);
        }
        return false;
    }
    //implement in the TxnManagerTStream
    protected abstract boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord tableRecord, SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType);
}
