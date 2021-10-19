package engine.table.tableRecords;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRecord implements Comparable<TableRecord>{
    private static final Logger LOG= LoggerFactory.getLogger(TableRecord.class);
    //final int size;
    //public Content content_;
    //public SchemaRecord record_;

    @Override
    public int compareTo(@NotNull TableRecord o) {
        return 0;
    }
}
