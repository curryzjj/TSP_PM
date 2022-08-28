package engine.transaction.common;

import engine.Meta.MetaTypes;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecord;
import engine.table.tableRecords.TableRecordRef;
import engine.transaction.TxnContext;
import engine.transaction.function.Condition;
import engine.transaction.function.Function;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class Operation implements Comparable<Operation>{
    public final TableRecord d_record;
    public final MetaTypes.AccessType accessType;

    public final TxnContext txn_context;
    public volatile TableRecordRef records_ref;//for cross-record dependency.
    public volatile SchemaRecordRef record_ref;//required by read-only: the place holder of the reading d_record.
    public final long bid;
    //required by READ_WRITE_and Condition.
    public final Function function;
    public List<DataBox> value_list;//required by write-only: the value_list to be used to update the d_record.
    //only update corresponding column.
    public long value;
    public int column_id;

    public final String table_name;
    //required by READ_WRITE.
    public volatile TableRecord s_record;//only if it is different from d_record.
    public volatile TableRecord[] condition_records;
    public Condition condition;
    public boolean[] success;
    public String name;
    public MetaTypes.OperationStateType operationStateType;
    public boolean isFailed = false;
    public Operation(String table_name, TxnContext txn_context, long bid, MetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref, Function function) {
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.function = function;
        this.s_record = d_record;
        this.record_ref = record_ref;//this holds events' record_ref.
        this.operationStateType = MetaTypes.OperationStateType.READY;
    }
    //for read only
    public Operation(String table_name, TxnContext txn_context, long bid, MetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.s_record = d_record;
        this.function = null;
        this.record_ref = record_ref;//this holds events' record_ref.
        this.operationStateType = MetaTypes.OperationStateType.READY;
    }
    //for write only
    public Operation(String table_name, TxnContext txn_context, long bid, MetaTypes.AccessType accessType, TableRecord record, List<DataBox> value_list) {
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.value_list = value_list;
        this.s_record = d_record;
        this.function = null;
        this.record_ref = null;
        this.operationStateType = MetaTypes.OperationStateType.READY;
    }
    /**
     * @param table_name
     * @param s_record
     * @param d_record
     * @param record_ref
     * @param bid
     * @param accessType
     * @param function
     * @param condition_records
     * @param condition
     * @param txn_context
     * @param success
     */
    public Operation(String table_name, TableRecord s_record, TableRecord d_record, SchemaRecordRef record_ref, long bid, MetaTypes.AccessType accessType, Function function, TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {
        this.table_name = table_name;
        this.s_record = s_record;
        this.d_record = d_record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.condition_records = condition_records;
        this.function = function;
        this.condition = condition;
        this.success = success;
        this.record_ref = record_ref;
        this.operationStateType = MetaTypes.OperationStateType.READY;
    }
    public Operation(String table_name, TxnContext txn_context, long bid, MetaTypes.AccessType accessType, TableRecord record, long value, int column_id) {
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.value = value;
        this.column_id = column_id;
        this.s_record = d_record;
        this.function = null;
        this.record_ref = null;
        this.operationStateType = MetaTypes.OperationStateType.READY;
    }
    /**
     * Update dest d_record by applying function of s_record.. It relys on MVCC to guarantee correctness.
     *
     * @param table_name
     * @param s_record
     * @param d_record
     * @param bid
     * @param accessType
     * @param function
     * @param txn_context
     * @param column_id
     */
    public Operation(String table_name, TableRecord s_record, TableRecord d_record, long bid, MetaTypes.AccessType accessType, Function function, TxnContext txn_context, int column_id) {
        this.table_name = table_name;
        this.d_record = d_record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.s_record = s_record;
        this.function = function;
        this.record_ref = null;
        this.column_id = column_id;
        this.operationStateType = MetaTypes.OperationStateType.READY;
    }
    public void stateTransition(MetaTypes.OperationStateType stateType) {
        this.operationStateType = stateType;
    }
    @Override
    public int compareTo(@NotNull Operation operation) {
        if (this.bid == (operation.bid)) {
            return this.d_record.getID() - operation.d_record.getID();//different records, don't care about its counter.
        } else
            return Long.compare(this.bid, operation.bid);
    }
}
