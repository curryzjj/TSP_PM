package engine.table.tableRecords;

import engine.table.RowID;
import engine.table.content.Content;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static utils.TransactionalProcessConstants.content_type;

public class TableRecord implements Comparable<TableRecord>, Serializable {
    private static final Logger LOG= LoggerFactory.getLogger(TableRecord.class);
    public Content content_;
    public SchemaRecord record_;
    public TableRecord(SchemaRecord record){
//        switch(content_type){
//
//        }
        record_=record;
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
}
