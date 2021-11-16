package engine.transaction.common;

import engine.transaction.log.LogRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class MyList<O> extends ConcurrentSkipListSet<O> {
    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getTable_name() {
        return table_name;
    }

    public List<LogRecord> getLog() {
        return Log;
    }

    private final String table_name;
    private final String primaryKey;
    public List<LogRecord> Log;


    public MyList(String table_name, String primaryKey) {
        this.table_name = table_name;
        this.primaryKey = primaryKey;
        this.Log=new ArrayList<>();
    }
}
