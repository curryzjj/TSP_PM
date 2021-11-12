package engine.storage.ImplStorageManager;

import engine.Exception.DatabaseException;
import engine.Meta.RegisteredKeyValueStateBackendMetaInfo;
import engine.Meta.RegisteredStateMetaInfoBase;
import engine.checkpoint.CheckpointOptions;
import engine.checkpoint.CheckpointStream.CheckpointStreamFactory;
import engine.checkpoint.RocksDBSnapshotStrategyBase;
import engine.checkpoint.SnapshotResult;
import engine.checkpoint.SnapshotStrategyRunner;
import engine.storage.AbstractStorageManager;
import engine.table.BaseTable;
import engine.table.ImplTable.ShareTable;
import engine.table.keyGroup.KeyGroupRange;
import engine.table.RecordSchema;
import engine.table.RowID;
import engine.table.datatype.serialize.Deserialize;
import engine.table.datatype.serialize.Serialize;
import engine.table.tableRecords.SchemaRecord;
import engine.table.tableRecords.TableRecord;;
import org.rocksdb.*;
import utils.CloseableRegistry.CloseableRegistry;
import utils.TransactionalProcessConstants;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static utils.TransactionalProcessConstants.SnapshotExecutionType.ASYNCHRONOUS;

public class RocksDBManager extends AbstractStorageManager {
    private RocksDB rocksDB;
    private List<ColumnFamilyHandle> columnFamilyHandleList;
    public Map<String ,ColumnFamilyHandle> columnFamilyHandles;
    public Map<String,DataBoxTypes> types;
    public Map<String, AtomicInteger> numRecords;
    private KeyGroupRange keyGroupRange;
    /** Shared wrapper for batch writes to the RocksDB instance. */
//    private final RocksDBWriteBatchWrapper writeBatchWrapper;
    /**
     * The checkpoint snapshot strategy, e.g., if we use full or incremental checkpoints, local
     * state, and so on.
     */
    private final RocksDBSnapshotStrategyBase<?> checkpointSnapshotStrategy;
    /**
     * Registry for all opened streams, so they can be closed if the task using this backend is
     * closed.
     */
    /**
     * Information about the k/v states, maintained in the order as we create them. This is used to
     * retrieve the column family that is used for a state and also for sanity checks when
     * restoring.
     */
    private final LinkedHashMap<String, RocksDBKvStateInfo> kvStateInformation;
    protected CloseableRegistry cancelStreamRegistry;
    public RocksDBManager(RocksDBSnapshotStrategyBase<?> checkpointSnapshotStrategy, CloseableRegistry cancelStreamRegistry){
        RocksDB.loadLibrary();
        tables=new ConcurrentHashMap<>();
        columnFamilyHandleList=new ArrayList<>();
        columnFamilyHandles=new HashMap<>();
        types=new HashMap<>();
        numRecords=new HashMap<>();
        this.checkpointSnapshotStrategy=checkpointSnapshotStrategy;
        this.cancelStreamRegistry=cancelStreamRegistry;
        try {
            rocksDB=RocksDB.open(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/"));
            columnFamilyHandleList.add(rocksDB.getDefaultColumnFamily());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        kvStateInformation = new LinkedHashMap<>();
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
        this.RegisterState(tableName,columnFamilyHandle,s);
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
    public void RegisterState(String tablename,ColumnFamilyHandle columnFamilyHandle,RecordSchema r){
        RegisteredKeyValueStateBackendMetaInfo MetaInfo=new RegisteredKeyValueStateBackendMetaInfo(TransactionalProcessConstants.BackendStateType.KEY_VALUE,tablename,r);
        RocksDBKvStateInfo stateInfo=new RocksDBKvStateInfo(columnFamilyHandle,MetaInfo);
        this.kvStateInformation.put(tablename,stateInfo);
    }
    public RunnableFuture<SnapshotResult> snapshot(final long checkpointId,
                                                   final long timestamp,
                                                   final CheckpointStreamFactory streamFactory,
                                                   CheckpointOptions checkpointOptions) throws Exception {
        return new SnapshotStrategyRunner<>(
                checkpointSnapshotStrategy.getDescription(),
                checkpointSnapshotStrategy,
                ASYNCHRONOUS,
                cancelStreamRegistry
                ).snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }
    public void createKeyGroupRange(){
        this.keyGroupRange=new KeyGroupRange(0,table_count-1);
    }
}