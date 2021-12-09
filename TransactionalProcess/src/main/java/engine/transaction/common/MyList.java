package engine.transaction.common;

import engine.log.LogRecord;

import java.util.concurrent.ConcurrentSkipListSet;

import static UserApplications.CONTROL.enable_states_partition;

public class MyList<O> extends ConcurrentSkipListSet<O> {
    public String getPrimaryKey() {
        return primaryKey;
    }
    public String getTable_name() {
        return table_name;
    }
    public LogRecord getLogRecord() {
        return logRecord;
    }

    public int getPartitionId() {
        return partitionId;
    }

    private final String table_name;
    private final String primaryKey;
    private final int partitionId;
    public LogRecord logRecord;


    public MyList(String table_name, String primaryKey,int partitionId) {
        this.table_name = table_name;
        this.primaryKey = primaryKey;
        this.partitionId = partitionId;
        if(enable_states_partition){
            this.logRecord =new LogRecord(primaryKey,table_name+"_"+ partitionId);
        }else{
            this.logRecord =new LogRecord(primaryKey,table_name);
        }
    }
}
