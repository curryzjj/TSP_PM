package engine.ImplDatabase;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.shapshot.CheckpointOptions;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.FsCheckpointStreamFactory;
import engine.shapshot.SnapshotResult;
import engine.storage.EventManager;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import org.rocksdb.RocksDBException;
import utils.CloseableRegistry.CloseableRegistry;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;

public class InMemeoryDatabase extends Database {
    public InMemeoryDatabase(Configuration configuration) {
        CloseableRegistry closeableRegistry=new CloseableRegistry();
        storageManager = new StorageManager(closeableRegistry,configuration);
        eventManager = new EventManager();
        if(OsUtils.isMac()){
            String snapshotPath=configuration.getString("snapshotTestPath");
            this.snapshotPath=new Path(System.getProperty("user.home").concat(snapshotPath));
        }else {
            String snapshotPath=configuration.getString("snapshotPath");
            this.snapshotPath=new Path(System.getProperty("user.home").concat(snapshotPath));
        }
        this.fs=new LocalFileSystem();
        this.checkpointOptions=new CheckpointOptions();
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
    public void Recovery() {

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
}
