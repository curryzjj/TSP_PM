package engine.ImplDatabase;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.log.LogResult;
import engine.log.LogStream.FsLogStreamFactory;
import engine.log.LogStream.LogStreamFactory;
import engine.log.logCommitRunner;
import engine.recovery.AbstractRecoveryManager;
import engine.shapshot.CheckpointOptions;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.FsCheckpointStreamFactory;
import engine.shapshot.SnapshotResult;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import engine.transaction.TxnProcessingEngine;
import org.rocksdb.RocksDBException;
import utils.CloseableRegistry.CloseableRegistry;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;

import static UserApplications.CONTROL.enable_parallel;
import static utils.TransactionalProcessConstants.CommitLogExecutionType.ASYNCHRONOUS;
import static utils.TransactionalProcessConstants.CommitLogExecutionType.SYNCHRONOUS;

public class InMemeoryDatabase extends Database {
    public InMemeoryDatabase(Configuration configuration) {
        CloseableRegistry closeableRegistry=new CloseableRegistry();
        storageManager = new StorageManager(closeableRegistry,configuration);
        if(OsUtils.isMac()){
            String snapshotPath=configuration.getString("snapshotTestPath");
            this.snapshotPath=new Path(System.getProperty("user.home").concat(snapshotPath));
            String WalPath=configuration.getString("WALTestPath");
            this.WalPath=new Path(System.getProperty("user.home").concat(WalPath));
        }else {
            String snapshotPath=configuration.getString("snapshotPath");
            this.snapshotPath=new Path(System.getProperty("user.home").concat(snapshotPath));
            String WalPath=configuration.getString("WALPath");
            this.WalPath=new Path(System.getProperty("user.home").concat(WalPath));
        }
        this.fs=new LocalFileSystem();
        this.checkpointOptions=new CheckpointOptions();
        this.txnProcessingEngine= TxnProcessingEngine.getInstance();
    }
    @Override
    public void createTable(RecordSchema tableSchema, String tableName, DataBoxTypes type) {
        try {
            storageManager.createTable(tableSchema, tableName,type);
        } catch (DatabaseException | RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void InsertRecord(String table, TableRecord record) throws DatabaseException, IOException {
        storageManager.InsertRecord(table, record);
    }

    @Override
    public void recoveryFromSnapshot(SnapshotResult lastSnapshotResult) throws IOException, ClassNotFoundException, DatabaseException {
        AbstractRecoveryManager.recoveryFromSnapshot(this,lastSnapshotResult);
    }
    @Override
    public long recoveryFromWAL(long globalLSN) throws IOException, ClassNotFoundException, DatabaseException, InterruptedException {
        if(enable_parallel){
            return AbstractRecoveryManager.parallelRecoveryFromWAL(this,WalPath,txnProcessingEngine.getNum_op(),globalLSN);
        }else{
            return AbstractRecoveryManager.recoveryFromWAL(this,WalPath,-1,globalLSN);
        }
    }

    @Override
    public boolean undoFromWAL() throws IOException, DatabaseException {
        return this.txnProcessingEngine.getWalManager().undoLog(this);
    }

    @Override
    public void reloadStateFromSnapshot(SnapshotResult snapshotResult) throws IOException, ClassNotFoundException, DatabaseException {
        this.storageManager.cleanAllTables();
        if(snapshotResult!=null){
            this.recoveryFromSnapshot(snapshotResult);
        }
    }

    @Override
    public void createKeyGroupRange() {
        this.storageManager.createKeyGroupRange();
    }

    @Override
    public RunnableFuture<SnapshotResult> snapshot(final long checkpointId, final long timestamp) throws Exception {
        CheckpointStreamFactory streamFactory=new FsCheckpointStreamFactory(16,
                16,
                snapshotPath,
                fs);
        RunnableFuture<SnapshotResult> snapshot = storageManager.snapshot(checkpointId,timestamp,streamFactory,checkpointOptions);
        return snapshot;
    }

    @Override
    public RunnableFuture<LogResult> commitLog(long globalLSN, long timestamp) throws IOException {
        CloseableRegistry cancelStreamRegistry=new CloseableRegistry();
        LogStreamFactory logStreamFactory=new FsLogStreamFactory(16,16,WalPath,fs);
        RunnableFuture<LogResult> commitLog=new logCommitRunner(cancelStreamRegistry,
                txnProcessingEngine.getWalManager(),SYNCHRONOUS
        ).commitLog(globalLSN, timestamp, logStreamFactory);
        return commitLog;
    }
}
