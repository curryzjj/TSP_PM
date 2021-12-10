package engine.transaction;

import engine.Exception.DatabaseException;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.transaction.function.Condition;
import engine.transaction.function.Function;
import scala.Int;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

public interface TxnManager {
    /**
     * Write-only
     * <p>
     * This API installes the given value_list to specific d_record and return.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param value
     * @param enqueue_time
     * @return
     * @throws DatabaseException
     */
    boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String key, List<DataBox> value, double[] enqueue_time) throws DatabaseException;
    boolean Asy_WriteRecord(TxnContext txn_context, String table, String id, long value, int column_id) throws DatabaseException;
    /**
     * Read-Modify_Write w/ read.
     *
     * @param txn_context
     * @param srcTable
     * @param record_ref  expect a return value_list from the store to support further computation in the application.
     * @param function    the pushdown function.
     * @return
     * @throws DatabaseException
     */
    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException;
    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException;
    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id) throws DatabaseException;
    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, boolean[] success) throws DatabaseException;
    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function) throws DatabaseException;
    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException;

    /**
     * Read-only
     * This API pushes a place-holder to the shared-store.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param record_ref   expect a return value_list from the store to support further computation in the application.
     * @param enqueue_time
     * @return
     * @throws DatabaseException
     */
    boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, double[] enqueue_time) throws DatabaseException;


    int start_evaluate(int taskId, long mark_ID) throws InterruptedException, BrokenBarrierException, IOException, DatabaseException;
}
