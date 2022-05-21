package engine.ImplDatabase;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.log.LogResult;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.FsCheckpointStreamFactory;
import engine.shapshot.SnapshotResult;
import engine.storage.ImplStorageManager.RocksDBManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import org.rocksdb.RocksDBException;
import utils.CloseableRegistry.CloseableRegistry;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;

public class RocksDBDatabase extends Database {
    public RocksDBDatabase(Configuration configuration) {
        CloseableRegistry closeableRegistry=new CloseableRegistry();
        storageManager = new RocksDBManager(closeableRegistry,configuration);
        if(OsUtils.isMac()){
            String snapshotPath=configuration.getString("snapshotTestPath");
            this.snapshotPath=new Path(System.getProperty("user.home").concat(snapshotPath));
        }else {
            String snapshotPath=configuration.getString("snapshotPath");
            this.snapshotPath=new Path(System.getProperty("user.home").concat(snapshotPath));
        }
        this.fs=new LocalFileSystem();
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
    public void recoveryFromSnapshot(SnapshotResult lastSnapshotResult) {

    }

    @Override
    public long recoveryFromWAL(long globalLSN) throws IOException {
        return 0;
    }

    @Override
    public boolean undoFromWAL() throws IOException, DatabaseException {
        return false;
    }

    @Override
    public void reloadStateFromSnapshot(SnapshotResult snapshotResult) throws IOException, ClassNotFoundException, DatabaseException {

    }

    public void createTableRange(int table_count){
        this.storageManager.createTableRange(table_count);
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
    public SnapshotResult parallelSnapshot(long checkpointId, long timestamp) throws Exception {
        return null;
    }

    @Override
    public RunnableFuture<LogResult> commitLog(long checkpointId, long timestamp) throws IOException {
        return null;
    }

    @Override
    public void setWalPath(String filename) {

    }
}
