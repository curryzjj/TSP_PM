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
    public Operation(String table_name, TxnContext txn_context, long bid, MetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref, Function function) {
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.function = function;
        this.s_record = d_record;
        this.record_ref = record_ref;//this holds events' record_ref.
    }
    @Override
    public int compareTo(@NotNull Operation operation) {
        if (this.bid == (operation.bid)) {
            return this.d_record.getID() - operation.d_record.getID();//different records, don't care about its counter.
        } else
            return Long.compare(this.bid, operation.bid);
    }
}
