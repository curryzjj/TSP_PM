package engine.storage.ImplStorageManager;

import System.tools.SortHelper;
import System.tools.StringHelper;
import System.util.Configuration;
import System.util.OsUtils;
import System.util.SpinLock;
import engine.Exception.DatabaseException;
import engine.Meta.RegisteredKeyValueStateBackendMetaInfo;
import engine.Meta.RegisteredStateMetaInfoBase;
import engine.shapshot.*;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.ImplSnapshotStrategy.InMemorySnapshotStrategy;
import engine.storage.AbstractStorageManager;
import engine.table.BaseTable;
import engine.table.ImplTable.ShareTable;
import engine.table.RecordSchema;
import engine.table.keyGroup.KeyGroupRange;
import engine.table.tableRecords.TableRecord;
import org.jetbrains.annotations.NotNull;
import utils.CloseableRegistry.CloseableRegistry;
import utils.ResourceGuard;
import utils.TransactionalProcessConstants;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;

import static java.nio.file.StandardOpenOption.APPEND;
import static utils.TransactionalProcessConstants.SnapshotExecutionType.SYNCHRONOUS;

public class StorageManager extends AbstractStorageManager {
    private KeyGroupRange keyGroupRange;
    private final InMemorySnapshotStrategyBase<?> checkpointSnapshotStrategy;
    protected CloseableRegistry cancelStreamRegistry;
    private final LinkedHashMap<String, InMemoryKvStateInfo> kvStateInformation;
    private String application;
    private int FTOptions;
    private int FailureTimes;
    private String FileDirectory;
    public StorageManager(CloseableRegistry cancelStreamRegistry, Configuration config){
        kvStateInformation=new LinkedHashMap<>();
        tables=new ConcurrentHashMap<>();
        this.cancelStreamRegistry=cancelStreamRegistry;
        this.checkpointSnapshotStrategy=initializeCheckpointStrategies(config);
        this.FileDirectory = config.getString("metrics.output");
        this.application = config.getString("application");
        this.FTOptions =  config.getInt("FTOptions");
        this.FailureTimes =  config.getInt("failureFrequency");
    }

    public BaseTable getTable(String tableName) throws DatabaseException {
        //TODO: getTable from Operators
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
        tables.put(tableName, new ShareTable(s, tableName,true));//here we decide which table to use.
        this.RegisterState(tableName, s);
        this.spinLocks.put(tableName, new SpinLock());
        table_count++;
    }
    public synchronized void cleanTable(String tableName) throws IOException {
        tables.get(tableName).clean();
    }

    @Override
    public synchronized void cleanTable(List<Integer> rangeId) throws IOException, DatabaseException {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        for(int id:rangeId){
            for (String s : tableNames) {
                if(StringHelper.isDigitStr(s) == id)
                    cleanTable(s);
            }
        }
    }

    @Override
    public synchronized void cleanAllTables() throws IOException {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        for (String s : tableNames) {
            cleanTable(s);
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
    public void RegisterState(String tableName,RecordSchema r){
        RegisteredKeyValueStateBackendMetaInfo MetaInfo = new RegisteredKeyValueStateBackendMetaInfo(TransactionalProcessConstants.BackendStateType.KEY_VALUE, tableName, r);
        InMemoryKvStateInfo inMemoryKvStateInfo = new InMemoryKvStateInfo(MetaInfo);
        this.kvStateInformation.put(tableName, inMemoryKvStateInfo);
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
    public void createTableRange(int table_count) {
        this.keyGroupRange = new KeyGroupRange(0,table_count-1);
        this.checkpointSnapshotStrategy.keyGroupRange = this.keyGroupRange;
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

    @Override
    public SnapshotStrategy.SnapshotResultSupplier parallelSnapshot(long checkpointId, long timestamp, @NotNull CheckpointStreamFactory streamFactory, @NotNull CheckpointOptions checkpointOptions) throws Exception {
        return new SnapshotStrategyRunner<>(
                checkpointSnapshotStrategy.getDescription(),
                checkpointSnapshotStrategy,
                SYNCHRONOUS,
                cancelStreamRegistry
        ).parallelSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Override
    public SnapshotStrategy.SnapshotResultSupplier asyncSnapshot(long checkpointId, long timestamp, int partitionId, @NotNull CheckpointStreamFactory streamFactory, @NotNull CheckpointOptions checkpointOptions) throws Exception {
        return new SnapshotStrategyRunner<>( checkpointSnapshotStrategy.getDescription(),
                checkpointSnapshotStrategy,
                SYNCHRONOUS,
                cancelStreamRegistry).asyncSnapshot(checkpointId,timestamp,partitionId,streamFactory,checkpointOptions);
    }

    @Override
    public void dumpDataBase() throws IOException {
        String filePath = this.FileDirectory + OsUtils.osWrapperPostFix("Database")
                + OsUtils.osWrapperPostFix(this.application)
                + OsUtils.osWrapperPostFix(String.valueOf(this.FTOptions))
                + OsUtils.osWrapperPostFix(String.valueOf(this.FailureTimes));
        File file = new File(filePath);
        file.mkdirs();
        if (file.exists())
            file.delete();
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
        ArrayList<String> tableNames = SortHelper.sortString(this.tables.keySet());
        for (String tableName: tableNames) {
            this.tables.get(tableName).DumpRecord(fileWriter);
        }
        fileWriter.flush();
        fileWriter.close();
    }

}
