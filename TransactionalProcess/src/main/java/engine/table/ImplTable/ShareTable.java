package engine.table.ImplTable;

import engine.Exception.DatabaseException;
import engine.index.BaseUnorderedIndex;
import engine.index.HashTableIndex;
import engine.index.StdUnorderedIndex;
import engine.table.BaseTable;
import engine.table.RecordSchema;
import engine.table.tableRecords.TableRecord;
import engine.table.tableRecords.TableRecords;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Iterator;

public class ShareTable extends BaseTable {

    private final BaseUnorderedIndex primary_index_;
    public ShareTable(RecordSchema schema, String table_Id,boolean is_thread_safe) {
        super(schema, table_Id);
        if(is_thread_safe){
            primary_index_=new HashTableIndex();
        }else{
            primary_index_=new StdUnorderedIndex();//not know why
        }
    }

    @Override
    public boolean InsertRecord(TableRecord record) throws DatabaseException {
        return false;
    }

    @Override
    public void clean() {

    }

    @Override
    public TableRecord SelectKeyRecord(String primary_key) {
        return primary_index_.SearchRecord(primary_key);
    }

    @Override
    public void SelectRecords(int idx_id, String secondary_key, TableRecords records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {

    }

    @NotNull
    @Override
    public Iterator<TableRecord> iterator() {
        return null;
    }
}
