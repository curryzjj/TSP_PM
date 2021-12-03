package engine.transaction;

import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.AbstractStorageManager;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static UserApplications.CONTROL.NUM_ITEMS;
import static UserApplications.CONTROL.enable_states_partition;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public abstract class TxnManagerDedicated implements TxnManager{
    public static final Logger LOG= LoggerFactory.getLogger(TxnManagerDedicated.class);
    public final AbstractStorageManager storageManager;
    private final String thisComponentId;
    private final long thread_id;
    private final long num_tasks;
    protected int delta;//range of each partition. depends on the number of op in the stage.
    protected int getTaskId(String key) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }


    public TxnManagerDedicated(AbstractStorageManager storageManager, String thisComponentId, int thread_Id, int num_tasks){
        this.storageManager = storageManager;
        this.thisComponentId = thisComponentId;
        this.thread_id = thread_Id;
        this.num_tasks = num_tasks;
        delta = (int) Math.ceil(NUM_ITEMS / (double) num_tasks);//NUM_ITEMS / tthread;
    }
    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType= MetaTypes.AccessType.READ_WRITE_READ;
        TableRecord tableRecord;
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+getTaskId(key);
        }else{
            tableName=srcTable;
        }
        tableRecord=storageManager.getTableRecords(tableName,key);
        if(tableRecord!=null){
            return Asy_ModifyRecord_ReadCC(txn_context,srcTable,tableRecord,record_ref,function,accessType);
        }
        return false;
    }
    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String primary_key, SchemaRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.READ_ONLY;
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+getTaskId(primary_key);
        }else{
            tableName=srcTable;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return Asy_ReadRecordCC(txn_context, primary_key, srcTable, t_record, record_ref, enqueue_time, accessType);
        } else {
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, List<DataBox> value, double[] enqueue_time) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.WRITE_ONLY;
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+getTaskId(primary_key);
        }else{
            tableName=srcTable;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return Asy_WriteRecordCC(txn_context, srcTable, t_record, primary_key, value, enqueue_time, accessType);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }
    //implement in the TxnManagerTStream
    protected abstract boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord tableRecord, SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType);
    protected abstract boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, SchemaRecordRef record_ref, double[] enqueue_time, MetaTypes.AccessType access_type);
    protected abstract boolean Asy_WriteRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, String primary_key, List<DataBox> value, double[] enqueue_time, MetaTypes.AccessType access_type);
}
