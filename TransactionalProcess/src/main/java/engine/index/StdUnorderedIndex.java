package engine.index;

import engine.table.tableRecords.TableRecord;

import java.util.Iterator;
import java.util.Set;

public class StdUnorderedIndex extends BaseUnorderedIndex{
    @Override
    public TableRecord SearchRecord(String primary_key) {
        return null;
    }

    @Override
    public boolean InsertRecord(String s, TableRecord record) {
        return false;
    }

    @Override
    public Set<String> getKeys() {
        return null;
    }

    @Override
    public void clean() {
    }

    @Override
    public Iterator<TableRecord> iterator() {
        return null;
    }
}
