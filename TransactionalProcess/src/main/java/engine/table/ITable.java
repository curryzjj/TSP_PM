package engine.table;

import engine.table.tableRecords.TableRecord;
import engine.table.tableRecords.TableRecords;

import java.io.Closeable;

public interface ITable extends Iterable<TableRecord>, Closeable {
    TableRecord SelectKeyRecord(String primary_key);
    void SelectRecords(int idx_id, String secondary_key, TableRecords records);
}
