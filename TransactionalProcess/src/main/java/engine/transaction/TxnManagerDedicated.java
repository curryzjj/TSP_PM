package engine.transaction;

import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.storage.AbstractStorageManager;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.transaction.function.Condition;
import engine.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Lock.PartitionedOrderLock;

import java.util.List;

import static UserApplications.CONTROL.*;

public abstract class TxnManagerDedicated implements TxnManager{
    public static final Logger LOG= LoggerFactory.getLogger(TxnManagerDedicated.class);
    public final AbstractStorageManager storageManager;
    private final String thisComponentId;
    private final long thread_id;
    private final long num_tasks;
    protected int delta;//range of each partition. depends on the number of op in the stage.
    protected int partition_delta;
    protected int getPartitionId(String key) {
        Integer _key = Integer.valueOf(key);
        return _key / partition_delta;
    }
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
        partition_delta = (int) Math.ceil(NUM_ITEMS / (double) PARTITION_NUM);//NUM_ITEMS / partition_num;
    }
    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType= MetaTypes.AccessType.READ_WRITE_READ;
        TableRecord tableRecord;
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+ getPartitionId(key);
        }else{
            tableName=srcTable;
        }
        tableRecord = storageManager.getTableRecords(tableName,key);
        if(tableRecord!=null){
            return Asy_ModifyRecord_ReadCC(txn_context,srcTable,tableRecord,record_ref,function,accessType);
        }
        return false;
    }
    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.READ_WRITE_COND_READ;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            String tableName = "";
            if(enable_states_partition){
                tableName = condition_sourceTable[i]+"_"+ getPartitionId(condition_source[i]);
            }else{
                tableName = condition_sourceTable[i];
            }
            condition_records[i] = storageManager.getTable(tableName).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
            if (condition_records[i] == null) {
                LOG.info("No record is found for condition source:" + condition_source[i]);
                return false;
            }
        }
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+ getPartitionId(key);
        }else{
            tableName=srcTable;
        }
        TableRecord s_record = storageManager.getTable(tableName).SelectKeyRecord(key);
        if (s_record != null) {
            return Asy_ModifyRecord_ReadCC(txn_context, srcTable, s_record, record_ref, function, condition_records, condition, accessType, success);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + key);
            return false;
        }
    }
    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String primary_key, SchemaRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.READ_ONLY;
        String tableName="";
        if(enable_states_partition){
            tableName = srcTable+"_"+ getPartitionId(primary_key);
        }else{
            tableName = srcTable;
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
            tableName=srcTable+"_"+ getPartitionId(primary_key);
        }else{
            tableName=srcTable;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return Asy_WriteRecordCC(txn_context, srcTable, t_record, primary_key, value, enqueue_time, accessType);
        } else {
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }
    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, long value, int column_id) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.WRITE_ONLY;
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+ getPartitionId(primary_key);
        }else{
            tableName=srcTable;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return Asy_WriteRecordCC(txn_context, primary_key, srcTable, t_record, value, column_id, accessType);//TxnContext txn_context, String srcTable, String primary_key, long value_list, int column_id
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }

    }
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.READ_WRITE;
        String tableName="";
        if(enable_states_partition){
            tableName = srcTable+"_"+ getPartitionId(source_key);
        }else{
            tableName = srcTable;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(source_key);

        if (t_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, t_record, t_record, function, accessType, column_id);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + source_key);
            return false;
        }
    }
    /**
     * condition on itself.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param function
     * @param condition
     * @param success
     * @return
     * @throws DatabaseException
     */
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, boolean[] success) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[1];
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+ getPartitionId(key);
        }else{
            tableName=srcTable;
        }
        TableRecord s_record = storageManager.getTable(tableName).SelectKeyRecord(key);
        condition_records[0] = s_record;
        if (s_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, s_record, function, condition_records, condition, accessType, success);
        } else {
            LOG.info("No record is found:" + key);
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            return false;
        }
    }
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            String tableName="";
            if(enable_states_partition){
                tableName=condition_sourceTable[i]+"_"+ getPartitionId(condition_source[i]);
            }else{
                tableName=condition_sourceTable[i];
            }
            condition_records[i] = storageManager.getTable(tableName).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
            if (condition_records[i] == null) {
                LOG.info("No record is found for condition source:" + condition_source[i]);
                return false;
            }
        }
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+ getPartitionId(key);
        }else{
            tableName=srcTable;
        }
        TableRecord s_record = storageManager.getTable(tableName).SelectKeyRecord(key);
        if (s_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, s_record, function, condition_records, condition, accessType, success);
        } else {
            LOG.info("No record is found:" + key);
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            return false;
        }
    }
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType = MetaTypes.AccessType.READ_WRITE;
        String tableName="";
        if(enable_states_partition){
            tableName=srcTable+"_"+ getPartitionId(key);
        }else{
            tableName=srcTable;
        }
        TableRecord t_record = storageManager.getTable(tableName).SelectKeyRecord(key);
        if (t_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, t_record, t_record, function, accessType, 1);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            return false;
        }
    }
    //implement in the TxnManagerTStream
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord tableRecord, SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType){
        throw new UnsupportedOperationException();
    }
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord s_record, SchemaRecordRef record_ref, Function function,
                                              TableRecord[] condition_records, Condition condition, MetaTypes.AccessType accessType, boolean[] success){
        throw new UnsupportedOperationException();
    }
    protected  boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, SchemaRecordRef record_ref, double[] enqueue_time, MetaTypes.AccessType access_type){
        throw new UnsupportedOperationException();
    }
    protected  boolean Asy_WriteRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, String primary_key, List<DataBox> value, double[] enqueue_time, MetaTypes.AccessType access_type){
        throw new UnsupportedOperationException();
    }
    protected  boolean Asy_WriteRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, long value, int column_id, MetaTypes.AccessType access_type){
        throw new UnsupportedOperationException();
    }
    protected  boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord t_record, TableRecord d_record, Function function, MetaTypes.AccessType accessType, int column_id){
        throw new UnsupportedOperationException();
    }
    protected  boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord s_record, TableRecord d_record, Function function, TableRecord[] condition_source, Condition condition, MetaTypes.AccessType accessType, boolean[] success){
        throw new UnsupportedOperationException();
    }
    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord s_record, Function function, TableRecord[] condition_source, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {
        return Asy_ModifyRecordCC(txn_context, srcTable, s_record, s_record, function, condition_source, condition, accessType, success);
    }
    //implement in the TxnManagerSStore
    public PartitionedOrderLock.LOCK getOrderLock(int p_id){
        throw new UnsupportedOperationException();
    }//partitioned. Global ordering can not be partitioned.

    @Override
    public boolean lock_ahead(String table_name, String key, MetaTypes.AccessType accessType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }
    @Override
    public boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void CommitTransaction(List<String> key) {
        throw new UnsupportedOperationException();
    }

}
