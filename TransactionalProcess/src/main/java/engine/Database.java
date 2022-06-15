package engine;

import System.FileSystem.FileSystem;
import System.FileSystem.Path;
import engine.Exception.DatabaseException;
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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;

import static UserApplications.CONTROL.enable_parallel;

public abstract class Database {
    public int numTransactions=0;//current number of activate transactions
    protected AbstractStorageManager storageManager;
    protected AbstractRecoveryManager recoveryManager;
    protected TxnProcessingEngine txnProcessingEngine;
    /* init in the EM */
    public static ExecutorService snapshotExecutor;
    protected Path snapshotPath;
    protected Path WalPath;
    protected String WalBathPath;
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
    public abstract void createTableRange(int table_count);

    /**
     * To recovery the DataBase from the snapshot
     * @param lastSnapshotResult
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws DatabaseException
     */
    public abstract void recoveryFromSnapshot(SnapshotResult lastSnapshotResult) throws IOException, ClassNotFoundException, DatabaseException, InterruptedException;
    public abstract void recoveryFromTargetSnapshot(SnapshotResult lastSnapshotResult, List<Integer> targetIds)throws IOException, ClassNotFoundException, DatabaseException, InterruptedException;
    /**
     * To recovery the DataBase from the WAL, and return the last committed globalLSN
     * @return
     */
    public abstract long recoveryFromWAL(long globalLSN) throws IOException, ClassNotFoundException, DatabaseException, InterruptedException;

    /**
     * To undo the DataBase from the WAL
     * @return
     */
    public abstract boolean undoFromWAL() throws IOException, DatabaseException;
    public abstract boolean undoFromWALToTargetOffset(List<Integer> recoveryIds,long targetOffset) throws IOException, DatabaseException;
    public abstract boolean cleanUndoLog(long offset);
    /**
     * Reload state from the lastSnapshot
     * @param snapshotResult
     */
    public abstract void reloadStateFromSnapshot(SnapshotResult snapshotResult) throws IOException, ClassNotFoundException, DatabaseException, InterruptedException;
    /**
     * To take a snapshot for the DataBase
     * @param checkpointId
     * @param timestamp
     * @return
     * @throws Exception
     */
    public abstract RunnableFuture<SnapshotResult> snapshot(final long checkpointId, final long timestamp) throws Exception;

    /**
     * To parallel take a snapshot
     * @param checkpointId
     * @param timestamp
     * @return
     * @throws Exception
     */
    public abstract SnapshotResult parallelSnapshot(final long checkpointId, final long timestamp) throws Exception;
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

    public void setCheckpointOptions(int partitionNum, int delta) {
        if(enable_parallel){
            this.checkpointOptions = new CheckpointOptions(partitionNum, delta);
        }else{
            this.checkpointOptions = new CheckpointOptions();
        }
    }

    public TxnProcessingEngine getTxnProcessingEngine() {
        return txnProcessingEngine;
    }

    public abstract void setWalPath(String filename) throws IOException;
}
