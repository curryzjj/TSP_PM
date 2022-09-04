package engine.index;

import engine.table.tableRecords.TableRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Set;

public abstract class BaseUnorderedIndex implements Iterable<TableRecord>{
    public abstract TableRecord SearchRecord(String primary_key);
    public abstract boolean InsertRecord(String key,TableRecord record);
    public abstract Set<String> getKeys();
    public abstract void clean();
    public abstract void DumpRecords(BufferedWriter bufferedWriter) throws IOException;
}
