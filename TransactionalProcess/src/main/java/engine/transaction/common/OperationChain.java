package engine.transaction.common;

import engine.Meta.MetaTypes;
import engine.log.LogRecord;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static UserApplications.CONTROL.enable_states_partition;

public class OperationChain<O> extends ConcurrentSkipListSet<O> {
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
    public AtomicBoolean needAbortHandling = new AtomicBoolean(false);
    public Queue<Operation> failedOperations = new ArrayDeque<>();
    public boolean isExecuted = false;

    public OperationChain(String table_name, String primaryKey, int partitionId) {
        this.table_name = table_name;
        this.primaryKey = primaryKey;
        this.partitionId = partitionId;
        if(enable_states_partition){
            this.logRecord = new LogRecord(primaryKey,table_name + "_" + partitionId);
        }else{
            this.logRecord = new LogRecord(primaryKey, table_name);
        }
    }
}
