package engine.table;

import engine.Exception.DatabaseException;
import engine.table.stats.TableStats;
import engine.table.tableRecords.TableRecord;
import engine.table.tableRecords.TableRecords;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseTable implements ITable {
    protected final AtomicInteger numRecords = new AtomicInteger();
    final int secondary_count_;
    private RecordSchema schema;
    private TableStats stats;
    private String table_Id;
    public BaseTable(RecordSchema schema,String table_Id){
        secondary_count_=0;
    }
    public abstract boolean InsertRecord(TableRecord record) throws DatabaseException;
    /**
     * Delete all records in the table.
     */
    public abstract void clean();
    public abstract Iterator<String> keyIterator();
    public abstract int keySize();
    public abstract Set getKey();
}
