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
        SchemaRecord schemaRecord = new SchemaRecord(record_.getValues());
        this.updateMultiValues(0, schemaRecord);
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
        return record_at_ts;
    }
    public void updateMultiValues(long ts, SchemaRecord record){
        versions.put(ts, record);
    }
    public void updateMultiValues(long ts){
        versions.put(ts, record_);
    }
    public void clean_map() {
        long ts = versions.lastKey();
        SchemaRecord lastRecord = versions.get(ts);
        versions.clear();
        record_.updateValues(lastRecord.getValues());
        versions.put(ts, lastRecord);
    }
    public TableRecord cloneTableRecord() throws IOException, ClassNotFoundException {
        return (TableRecord) Serialize.cloneObject(this);
    }
    public void undoRecord(TableRecord tableRecord) {
        this.record_.updateValues(tableRecord.record_.getValues());
        this.versions = tableRecord.versions;
    }
}
