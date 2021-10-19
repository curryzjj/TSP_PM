package engine.storage;

import engine.Database;
import engine.Exception.DatabaseException;
import engine.ImplDatabase.InMemeoryDatabase;
import engine.table.BaseTable;
import engine.table.ImplTable.ShareTable;
import engine.table.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StorageManager {
    public Map<String, BaseTable> tables;
    int table_count;
    public StorageManager(){
        tables=new ConcurrentHashMap<>();
    }
    public BaseTable getTable(String tableName) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table: " + tableName + " does not exist");
        }
        return tables.get(tableName);
    }
    public synchronized  void createTable(RecordSchema s, String tableName, Database db) throws DatabaseException{
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table name already exists");
        }
        //TODO:switch different tables
        if(db instanceof InMemeoryDatabase){
            tables.put(tableName, new ShareTable(s,tableName,true));//here we decide which table to use.
        }
        table_count++;
    }
    /**
     * Delete a table in this database.
     *
     * @param tableName the name of the table
     * @return true if the database was successfully deleted
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
}
