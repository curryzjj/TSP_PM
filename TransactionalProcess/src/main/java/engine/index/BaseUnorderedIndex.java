package engine.index;

import engine.table.tableRecords.TableRecord;

public abstract class BaseUnorderedIndex implements Iterable<TableRecord>{
    public abstract TableRecord SearchRecord(String primary_key);
    public abstract boolean InsertRecord(String key,TableRecord record);
}
