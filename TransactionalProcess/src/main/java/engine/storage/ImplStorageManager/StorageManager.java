package engine.storage.ImplStorageManager;

import System.util.Configuration;
import engine.Exception.DatabaseException;
import engine.Meta.RegisteredKeyValueStateBackendMetaInfo;
import engine.Meta.RegisteredStateMetaInfoBase;
import engine.shapshot.CheckpointOptions;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.ImplSnapshotStrategy.InMemorySnapshotStrategy;
import engine.shapshot.InMemorySnapshotStrategyBase;
import engine.shapshot.SnapshotResult;
import engine.shapshot.SnapshotStrategyRunner;
import engine.storage.AbstractStorageManager;
import engine.table.BaseTable;
import engine.table.ImplTable.ShareTable;
import engine.table.RecordSchema;
import engine.table.keyGroup.KeyGroupRange;
import engine.table.tableRecords.TableRecord;
import utils.CloseableRegistry.CloseableRegistry;
import utils.ResourceGuard;
import utils.TransactionalProcessConstants;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;

import static utils.TransactionalProcessConstants.SnapshotExecutionType.SYNCHRONOUS;

public class StorageManager extends AbstractStorageManager {
    private KeyGroupRange keyGroupRange;
    private final InMemorySnapshotStrategyBase<?> checkpointSnapshotStrategy;
    protected CloseableRegistry cancelStreamRegistry;
    private final LinkedHashMap<String,InMemoryKvStateInfo> kvStateInformation;
    public StorageManager(CloseableRegistry cancelStreamRegistry, Configuration config){
        kvStateInformation=new LinkedHashMap<>();
        tables=new ConcurrentHashMap<>();
        this.cancelStreamRegistry=cancelStreamRegistry;
        this.checkpointSnapshotStrategy=initializeCheckpointStrategies(config);
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
        this.RegisterState(tableName,s);
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
    public void RegisterState(String tablename,RecordSchema r){
        RegisteredKeyValueStateBackendMetaInfo MetaInfo=new RegisteredKeyValueStateBackendMetaInfo(TransactionalProcessConstants.BackendStateType.KEY_VALUE,tablename,r);
        InMemoryKvStateInfo inMemoryKvStateInfo=new InMemoryKvStateInfo(MetaInfo);
        this.kvStateInformation.put(tablename,inMemoryKvStateInfo);
    }

    /**In-Memory specific information about the K/V states */
    public static class InMemoryKvStateInfo implements AutoCloseable{
        public final RegisteredStateMetaInfoBase metaInfo;
        public InMemoryKvStateInfo(RegisteredStateMetaInfoBase metaInfo) {
            this.metaInfo = metaInfo;
        }
        @Override
        public void close() throws Exception {

        }
    }
    @Override
    public void createKeyGroupRange() {
        this.keyGroupRange=new KeyGroupRange(0,table_count-1);
        this.checkpointSnapshotStrategy.keyGroupRange=this.keyGroupRange;
    }
    /**
     * initialize the snapshot strategies
     */
    private InMemorySnapshotStrategyBase initializeCheckpointStrategies(Configuration config) {
        ResourceGuard resourceGuard=new ResourceGuard();
        return new InMemorySnapshotStrategy(tables,resourceGuard,kvStateInformation,keyGroupRange);
    }
    @Override
    public RunnableFuture<SnapshotResult> snapshot(long checkpointId, long timestamp, CheckpointStreamFactory streamFactory, CheckpointOptions checkpointOptions) throws Exception {
        return new SnapshotStrategyRunner<>(
                checkpointSnapshotStrategy.getDescription(),
                checkpointSnapshotStrategy,
                SYNCHRONOUS,
                cancelStreamRegistry
        ).snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }
}
