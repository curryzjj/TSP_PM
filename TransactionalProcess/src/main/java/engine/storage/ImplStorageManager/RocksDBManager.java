package engine.storage.ImplStorageManager;

import engine.Exception.DatabaseException;
import engine.Meta.RegisteredStateMetaInfoBase;
import engine.storage.AbstractStorageManager;
import engine.table.BaseTable;
import engine.table.ImplTable.ShareTable;
import engine.table.RecordSchema;
import engine.table.RowID;
import engine.table.datatype.serialize.Deserialize;
import engine.table.datatype.serialize.Serialize;
import engine.table.tableRecords.SchemaRecord;
import engine.table.tableRecords.TableRecord;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBManager extends AbstractStorageManager {
    private RocksDB rocksDB;
    private List<ColumnFamilyHandle> columnFamilyHandleList;
    public Map<String ,ColumnFamilyHandle> columnFamilyHandles;
    public Map<String,DataBoxTypes> types;
    public Map<String, AtomicInteger> numRecords;
    public RocksDBManager(){
        RocksDB.loadLibrary();
        tables=new ConcurrentHashMap<>();
        columnFamilyHandleList=new ArrayList<>();
        columnFamilyHandles=new HashMap<>();
        types=new HashMap<>();
        numRecords=new HashMap<>();
        try {
            rocksDB=RocksDB.open(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/"));
            columnFamilyHandleList.add(rocksDB.getDefaultColumnFamily());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void createTable(RecordSchema s, String tableName, DataBoxTypes type) throws DatabaseException, RocksDBException {
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table name already exists");
        }
        ColumnFamilyDescriptor columnFamilyDescriptor=new ColumnFamilyDescriptor(tableName.getBytes(StandardCharsets.UTF_8));
        ColumnFamilyHandle columnFamilyHandle=rocksDB.createColumnFamily(columnFamilyDescriptor);
        columnFamilyHandleList.add(columnFamilyHandle);
        columnFamilyHandles.put(tableName,columnFamilyHandle);
        types.put(tableName,type);
        numRecords.put(tableName,new AtomicInteger());
        tables.put(tableName,new ShareTable(s,tableName,true));
        table_count++;
    }

    @Override
    public BaseTable getTable(String tableName) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table: " + tableName + " does not exist");
        }
        return tables.get(tableName);
    }

    @Override
    public TableRecord getTableRecords(String tableName,String key) throws DatabaseException {
        if(this.getTable(tableName).SelectKeyRecord(key)==null){
            byte[] k=key.getBytes(StandardCharsets.UTF_8);
            ColumnFamilyHandle c=columnFamilyHandles.get(tableName);
            DataBoxTypes dataBoxType=types.get(tableName);
            TableRecord tableRecord=null;
            try {
                byte[] v=rocksDB.get(c,k);
                tableRecord= Deserialize.Deserialize2TableRecord(v,TableRecord.class.getClassLoader());
            } catch (RocksDBException | IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            this.getTable(tableName).InsertRecord(tableRecord);
            return this.getTable(tableName).SelectKeyRecord(key);
        }else{
            return this.getTable(tableName).SelectKeyRecord(key);
        }
    }

    @Override
    public void InsertRecord(String tableName, TableRecord record) throws DatabaseException, IOException {
        ColumnFamilyHandle c=columnFamilyHandles.get(tableName);
        SchemaRecord schemaRecord=record.record_;
        byte[] key=schemaRecord.GetPrimaryKey().getBytes(StandardCharsets.UTF_8);
        int records=numRecords.get(tableName).getAndIncrement();
        record.setID(new RowID(records));//which row
        byte[] value= Serialize.serializeObject(record);
        try {
            rocksDB.put(c,key,value);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
    /**
     * commitTable to the RocksDB
     * @param tableName the name of the table
     */
    public boolean commitTable(String tableName) throws IOException, DatabaseException {
        if (!tables.containsKey(tableName)) {
            return false;
        }
        Iterator<TableRecord> tableRecordIterator=tables.get(tableName).iterator();
        while (tableRecordIterator.hasNext()){
            this.InsertRecord(tableName,tableRecordIterator.next());
        }
        return true;
    }
    public void cleanTable(String tableName) throws DatabaseException {
        this.getTable(tableName).clean();
    }
    /**
     * commitAllTables to the RocksDB
     */
    public void commitAllTables() throws IOException, DatabaseException {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        for (String s : tableNames) {
            if (commitTable(s)){
                cleanTable(s);
            }
        }
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

    @Override
    public synchronized void close() throws IOException {
        try {
            rocksDB.deleteFile(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/"));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
    /**Rocks DB specific information about the K/V states. */
    public static class RocksDBKvStateInfo implements AutoCloseable{
        public final ColumnFamilyHandle columnFamilyHandle;
        public final RegisteredStateMetaInfoBase metaInfo;

        public RocksDBKvStateInfo(ColumnFamilyHandle columnFamilyHandle, RegisteredStateMetaInfoBase metaInfo) {
            this.columnFamilyHandle = columnFamilyHandle;
            this.metaInfo = metaInfo;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
