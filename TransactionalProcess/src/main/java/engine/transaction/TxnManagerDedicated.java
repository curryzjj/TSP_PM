package engine.transaction;

import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.AbstractStorageManager;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TxnManagerDedicated implements TxnManager{
    public static final Logger LOG= LoggerFactory.getLogger(TxnManagerDedicated.class);
    protected final AbstractStorageManager storageManager;
    private final String thisComponentId;
    private final long thread_id;
    private final long num_tasks;


    public TxnManagerDedicated(AbstractStorageManager storageManager, String thisComponentId, int thread_Id, int num_tasks){
        this.storageManager = storageManager;
        this.thisComponentId = thisComponentId;
        this.thread_id = thread_Id;
        this.num_tasks = num_tasks;
    }
    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType= MetaTypes.AccessType.READ_WRITE_READ;
        TableRecord tableRecord=storageManager.getTableRecords(srcTable,key);
        if(tableRecord!=null){
            return Asy_ModifyRecord_ReadCC(txn_context,srcTable,tableRecord,record_ref,function,accessType);
        }
        return false;
    }
    //implement in the TxnManagerTStream
    protected abstract boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord tableRecord, SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType);
}
