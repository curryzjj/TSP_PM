package engine;

import engine.Exception.DatabaseException;
import engine.checkpoint.CheckpointManager;
import engine.recovery.RecoveryManager;
import engine.storage.AbstractStorageManager;
import engine.storage.EventManager;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;

public abstract class Database {
    public int numTransactions=0;//current number of activate transactions
    protected AbstractStorageManager storageManager;
    protected EventManager eventManager;
    protected RecoveryManager recoveryManager;
    protected CheckpointManager checkpointManager;
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
    public abstract void createTable(RecordSchema tableSchema, String tableName, DataBoxTypes type);
    public abstract void InsertRecord(String table, TableRecord record) throws DatabaseException, IOException;
    public abstract void Recovery();
    public AbstractStorageManager getStorageManager() {
        return storageManager;
    }
    public EventManager getEventManager() {
        return eventManager;
    }
    public RecoveryManager getRecoveryManager() {
        return recoveryManager;
    }
}
