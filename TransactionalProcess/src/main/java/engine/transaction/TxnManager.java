package engine.transaction;

import engine.Exception.DatabaseException;
import engine.table.tableRecords.SchemaRecordRef;
import engine.transaction.function.Function;

public interface TxnManager {
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
}
