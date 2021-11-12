package engine.storage.ImplStorageManager;

import engine.Database;
import engine.Exception.DatabaseException;
import engine.ImplDatabase.InMemeoryDatabase;
import engine.storage.AbstractStorageManager;
import engine.table.BaseTable;
import engine.table.ImplTable.ShareTable;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StorageManager extends AbstractStorageManager {
    public StorageManager(){
        tables=new ConcurrentHashMap<>();
    }
    public BaseTable getTable(String tableName) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table: " + tableName + " does not exist");
        }
        return tables.get(tableName);
    }

    @Override
    public TableRecord getTableRecords(String tableName,String key) throws DatabaseException {
        return this.getTable(tableName).SelectKeyRecord(key);
    }

    public synchronized  void createTable(RecordSchema s, String tableName, DataBoxTypes type) throws DatabaseException{
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table name already exists");
        }
        //TODO:switch different tables
            tables.put(tableName, new ShareTable(s,tableName,true));//here we decide which table to use.
        table_count++;
    }
    /**
     * Delete a table in this database.
     *
     * @param tableName the name of the table
     */
    public synchronized boolean dropTable(String tableName) throws IOException {
        if (!tables.containsKey(tableName)) {
            return false;
        }
        tables.get(tableName).close();
        tables.remove(tableName);
        return true;
    }
    /**
     * Delete all tables from this database.
     */
    public synchronized void dropAllTables() throws IOException {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        for (String s : tableNames) {
            dropTable(s);
        }
    }
    /**
     * Close this database.
     */
    public synchronized void close() throws IOException {
        for (BaseTable t : tables.values()) {
            t.close();
        }
        tables.clear();
    }

    public void InsertRecord(String tableName, TableRecord record) throws DatabaseException {
        BaseTable tab = getTable(tableName);
        tab.InsertRecord(record);
    }

    @Override
    public void createKeyGroupRange() {

    }
}
