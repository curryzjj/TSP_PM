package engine.index;

import engine.table.tableRecords.TableRecord;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class HashTableIndex extends BaseUnorderedIndex{
    private ConcurrentHashMap<String, TableRecord> hash_index_ =new ConcurrentHashMap<>();

    @Override
    public TableRecord SearchRecord(String primary_key) {
        return hash_index_.get(primary_key);
    }

    @Override
    public boolean InsertRecord(String key, TableRecord record) {
        hash_index_.put(key, record);
        return true;
    }
    public Set<String> getKeys(){
        return hash_index_.keySet();
    }

    @Override
    public void clean() {
        hash_index_.clear();
    }

    @NotNull
    @Override
    public Iterator<TableRecord> iterator() {
        return hash_index_.values().iterator();
    }
}
