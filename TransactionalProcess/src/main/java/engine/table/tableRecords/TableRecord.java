package engine.table.tableRecords;

import engine.table.RowID;
import engine.table.datatype.serialize.Serialize;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class TableRecord implements Comparable<TableRecord>, Serializable {
    private static final long serialVersionUID = -6940843588636593468L;
    public ConcurrentSkipListMap<Long, SchemaRecord> versions = new ConcurrentSkipListMap<>();//TODO: In fact... there can be at most only one write to the d_record concurrently. It is safe to just use sorted hashmap.
    public SchemaRecord record_;
    public TableRecord(SchemaRecord record){
        record_ = record;
        this.updateMultiValues(0, record);
    }
    @Override
    public int compareTo(@NotNull TableRecord o) {
        return Math.toIntExact(record_.getId().getID() - o.record_.getId().getID());
    }
    public void setID(RowID ID) {
        this.record_.setID(ID);
    }
    public int getID() {
        return record_.getId().getID();
    }
    public SchemaRecord readPreValues(long ts){
        SchemaRecord record_at_ts = null;
        Map.Entry<Long,SchemaRecord> entry = versions.lowerEntry(ts);
        if (entry != null){
            record_at_ts = entry.getValue();
        }else{
            record_at_ts = versions.get(ts);
        }
        if (record_at_ts == null && ts > 200000) {
            System.out.println();
        }
        return record_at_ts;
    }
    public void updateMultiValues(long ts, SchemaRecord record){
        versions.put(ts,record);
    }
    public void clean_map() {
        versions.headMap(versions.lastKey(), false).clear();
    }
    public void clean_map(long ts) {
        SchemaRecord record_at_ts = null;
        Map.Entry<Long,SchemaRecord> entry = versions.lowerEntry(ts);
        if (entry != null){
            versions.clear();
            versions.put(entry.getKey(), entry.getValue());
        }else{
            record_at_ts = versions.get(ts);
            versions.clear();
            versions.put(ts,record_at_ts);
        }

    }
    public TableRecord cloneTableRecord() throws IOException, ClassNotFoundException {
        return (TableRecord) Serialize.cloneObject(this);
    }
}
