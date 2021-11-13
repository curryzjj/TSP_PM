package engine.ImplDatabase;

import System.util.Configuration;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.storage.EventManager;
import engine.storage.ImplStorageManager.RocksDBManager;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import org.rocksdb.RocksDBException;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;

public class RocksDBDatabase extends Database {
    public RocksDBDatabase(Configuration configuration) {
        storageManager = new RocksDBManager(null,null,configuration);
        eventManager = new EventManager();
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
    public void createKeyGroupRange(){
        this.storageManager.createKeyGroupRange();
    }
}
