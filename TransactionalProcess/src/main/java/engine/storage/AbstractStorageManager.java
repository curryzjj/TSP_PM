package engine.storage;

import engine.Exception.DatabaseException;
import engine.shapshot.CheckpointOptions;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.SnapshotResult;
import engine.shapshot.SnapshotStrategy;
import engine.table.BaseTable;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import org.rocksdb.RocksDBException;
import utils.TransactionalProcessConstants.DataBoxTypes;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

public abstract class AbstractStorageManager {
    public Map<String, BaseTable> tables = null;
    public int table_count = 0;
    public abstract BaseTable getTable(String tableName) throws DatabaseException;
    public abstract TableRecord getTableRecords(String tableName,String key) throws DatabaseException;
    public synchronized void createTable(RecordSchema s, String tableName, DataBoxTypes type) throws DatabaseException, RocksDBException {};
    /**
     * Delete a table in this database.
     *
     * @param tableName the name of the table
     */
    public synchronized boolean dropTable(String tableName) throws IOException{return false;};
    /**
     * Delete all tables from this database.
     */
    public synchronized void dropAllTables() throws IOException{};
    public synchronized void cleanTable(String tableName) throws IOException, DatabaseException {};
    public synchronized void cleanTable(List<Integer> rangeId) throws IOException, DatabaseException {};
    public synchronized void cleanAllTables() throws IOException{};
    /**
     * Delete a table in this database.
     *
     * @param tableName the name of the table
     */
    public boolean commitTable(String tableName) throws IOException,DatabaseException{return false;};
    /**
     * Delete all tables from this database.
     */
    public void commitAllTables() throws IOException,DatabaseException{};
    /**
     * Close this database.
     */
    public synchronized void close() throws IOException{};
    public abstract void InsertRecord(String tableName, TableRecord record) throws DatabaseException, IOException;
    public abstract void createTableRange(int table_count);
    public abstract RunnableFuture<SnapshotResult> snapshot(final long checkpointId,
                                                                   final long timestamp,
                                                                   final CheckpointStreamFactory streamFactory,
                                                                   CheckpointOptions checkpointOptions) throws Exception;
    public abstract SnapshotStrategy.SnapshotResultSupplier parallelSnapshot(long checkpointId,
                                                                                          long timestamp,
                                                                                          @Nonnull CheckpointStreamFactory streamFactory,
                                                                                          @Nonnull CheckpointOptions checkpointOptions) throws Exception;
    public abstract SnapshotStrategy.SnapshotResultSupplier asyncSnapshot(long checkpointId,
                                                                          long timestamp,
                                                                          int partitionId,
                                                                          @Nonnull CheckpointStreamFactory streamFactory,
                                                                          @Nonnull CheckpointOptions checkpointOptions) throws Exception;
    public abstract void dumpDataBase() throws IOException;
}
