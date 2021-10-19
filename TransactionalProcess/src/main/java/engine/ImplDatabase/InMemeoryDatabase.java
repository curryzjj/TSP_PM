package engine.ImplDatabase;

import engine.Database;
import engine.Exception.DatabaseException;
import engine.recovery.RecoveryManager;
import engine.storage.EventManager;
import engine.storage.StorageManager;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;

public class InMemeoryDatabase extends Database {
    public InMemeoryDatabase(String path) {
        storageManager = new StorageManager();
        eventManager = new EventManager();
    }
    @Override
    public void createTable(RecordSchema tableSchema, String tableName) {
        try {
            storageManager.createTable(tableSchema, tableName,this);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void InsertRecord(String table, TableRecord record) throws DatabaseException {

    }

    @Override
    public void Recovery() {

    }
}
