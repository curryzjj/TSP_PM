package engine.storage;

import engine.Exception.DatabaseException;
import engine.shapshot.CheckpointOptions;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.SnapshotResult;
import engine.table.BaseTable;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import org.rocksdb.RocksDBException;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

public abstract class AbstractStorageManager {
    public Map<String, BaseTable> tables = null;
    public int table_count = 0;
    public abstract BaseTable getTable(String tableName) throws DatabaseException;
    public abstract TableRecord getTableRecords(String tableName,String key) throws DatabaseException;
    public synchronized  void createTable(RecordSchema s, String tableName, DataBoxTypes type) throws DatabaseException, RocksDBException {};
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
    public abstract void createKeyGroupRange();
    public abstract RunnableFuture<SnapshotResult> snapshot(final long checkpointId,
                                                                   final long timestamp,
                                                                   final CheckpointStreamFactory streamFactory,
                                                                   CheckpointOptions checkpointOptions) throws Exception;
}
