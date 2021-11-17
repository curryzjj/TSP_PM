package engine;

import System.FileSystem.FileSystem;
import System.FileSystem.Path;
import engine.Exception.DatabaseException;
import engine.shapshot.CheckpointManager;
import engine.recovery.AbstractRecoveryManager;
import engine.shapshot.CheckpointOptions;
import engine.storage.AbstractStorageManager;
import engine.storage.EventManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import utils.LocalRecoveryConfig;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;

public abstract class Database {
    public int numTransactions=0;//current number of activate transactions
    protected AbstractStorageManager storageManager;
    protected EventManager eventManager;
    protected AbstractRecoveryManager recoveryManager;
    protected CheckpointManager checkpointManager;
    protected Path snapshotPath;
    protected FileSystem fs;
    protected CheckpointOptions checkpointOptions;
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
    public AbstractRecoveryManager getRecoveryManager() {
        return recoveryManager;
    }
    public abstract void createKeyGroupRange();
    public abstract void snapshot(final long checkpointId,final long timestamp) throws Exception;
}
