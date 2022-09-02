package engine.table;

import engine.Exception.DatabaseException;
import engine.table.stats.TableStats;
import engine.table.tableRecords.TableRecord;
import engine.table.tableRecords.TableRecords;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
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
        this.schema=schema;
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

    public abstract void DumpRecord(BufferedWriter bufferedWriter) throws IOException;

    public RecordSchema getSchema() {
        return schema;
    }
}
