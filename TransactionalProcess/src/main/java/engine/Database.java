package engine;

import System.FileSystem.FileSystem;
import System.FileSystem.Path;
import engine.Exception.DatabaseException;
import engine.log.LogRecord;
import engine.log.LogResult;
import engine.recovery.AbstractRecoveryManager;
import engine.shapshot.CheckpointOptions;
import engine.shapshot.SnapshotResult;
import engine.storage.AbstractStorageManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import engine.transaction.TxnProcessingEngine;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;

public abstract class Database {
    public int numTransactions=0;//current number of activate transactions
    protected AbstractStorageManager storageManager;
    protected AbstractRecoveryManager recoveryManager;
    protected TxnProcessingEngine txnProcessingEngine;
    protected Path snapshotPath;
    protected Path WalPath;
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
    public AbstractStorageManager getStorageManager() {
        return storageManager;
    }
    public AbstractRecoveryManager getRecoveryManager() {
        return recoveryManager;
    }
    public abstract void createKeyGroupRange();

    /**
     * To recovery the DataBase from the snapshot
     * @param lastSnapshotResult
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws DatabaseException
     */
    public abstract void recoveryFromSnapshot(SnapshotResult lastSnapshotResult) throws IOException, ClassNotFoundException, DatabaseException;

    /**
     * To recovery the DataBase from the WAL, and return the last committed globalLSN
     * @return
     */
    public abstract long recoveryFromWAL() throws IOException, ClassNotFoundException, DatabaseException;

    /**
     * To undo the DataBase from the WAL
     * @return
     */
    public abstract boolean undoFromWAL() throws IOException, DatabaseException;

    /**
     * Reload state from the lastSnapshot
     * @param snapshotResult
     */
    public abstract void reloadStateFromSnapshot(SnapshotResult snapshotResult) throws IOException, ClassNotFoundException, DatabaseException;
    /**
     * To take a snapshot for the DataBase
     * @param checkpointId
     * @param timestamp
     * @return
     * @throws Exception
     */
    public abstract RunnableFuture<SnapshotResult> snapshot(final long checkpointId, final long timestamp) throws Exception;
    /**
     * To commit the update log for the group of transactions
     * @param globalLSN
     * @param timestamp
     * @return
     */
    public abstract RunnableFuture<LogResult> commitLog(final long globalLSN, final long timestamp) throws IOException;

    public FileSystem getFs() {
        return fs;
    }
}
