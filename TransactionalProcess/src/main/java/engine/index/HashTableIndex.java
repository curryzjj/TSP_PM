package engine.index;

import System.tools.SortHelper;
import engine.table.tableRecords.TableRecord;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class HashTableIndex extends BaseUnorderedIndex{
    private ConcurrentHashMap<String, TableRecord> hash_index_ = new ConcurrentHashMap<>();

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

    @Override
    public void DumpRecords(BufferedWriter bufferedWriter) throws IOException {
        ArrayList<String> keys = SortHelper.sortStringByInt(hash_index_.keySet());
        for (String key : keys) {
            hash_index_.get(key).clean_map();
            bufferedWriter.write(hash_index_.get(key).record_.toString());
            bufferedWriter.write("\n");
        }
    }

    @NotNull
    @Override
    public Iterator<TableRecord> iterator() {
        return hash_index_.values().iterator();
    }
}
