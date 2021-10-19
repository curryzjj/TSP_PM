package engine;

import engine.Exception.DatabaseException;
import engine.recovery.RecoveryManager;
import engine.storage.EventManager;
import engine.storage.StorageManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;

import java.io.IOException;

public abstract class Database {
    public int numTransactions=0;//current number of activate transactions
    protected StorageManager storageManager;
    protected EventManager eventManager;
    protected RecoveryManager recoveryManager;
    /**
     * Close this database.
     */
    public synchronized void close() throws IOException {
        storageManager.close();
    }
    /**
     *
     */
    public void dropAllTables() throws IOException {
        storageManager.dropAllTables();
    }
    /**
     * @param tableSchema
     * @param tableName
     */
    public abstract void createTable(RecordSchema tableSchema, String tableName);
    public abstract void InsertRecord(String table, TableRecord record) throws DatabaseException;
    public abstract void Recovery();
    public StorageManager getStorageManager() {
        return storageManager;
    }
    public EventManager getEventManager() {
        return eventManager;
    }
    public RecoveryManager getRecoveryManager() {
        return recoveryManager;
    }
}
