package engine.log;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.log.LogStream.*;
import engine.transaction.common.MyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static UserApplications.CONTROL.enable_parallel;

public class WALManager {
    private static final Logger LOG= LoggerFactory.getLogger(WALManager.class);
    private static final String DESCRIPTION="Asynchronous commit update log";
    private ConcurrentHashMap<String,LogRecords_in_range> holder_by_tableName;
    private int partitionNum;
    /* init in the EM */
    public static ExecutorService writeExecutor;
    public class LogRecords_in_range{
        public ConcurrentHashMap<Integer, Vector<LogRecord>> holder_by_range=new ConcurrentHashMap<>();
        public HashMap<String,Boolean> hasKey;
        public LogRecords_in_range(Integer num_op){
            int i;
            for (i=0;i<num_op;i++){
                holder_by_range.put(i,new Vector<>());
                this.hasKey=new HashMap<>();
            }
        }
    }
    public WALManager(int partitionNum){
        this.partitionNum =partitionNum;
        this.holder_by_tableName=new ConcurrentHashMap<>();
    }
    public boolean isEmpty(){
        boolean flag=true;
        for(LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Vector<LogRecord> logRecords:logRecordsInRange.holder_by_range.values()){
                if (!logRecords.isEmpty()){
                    flag=false;
                }
            }
        }
        return flag;
    }
    public void setHolder_by_tableName(String tableName,int num_op) {
        this.holder_by_tableName.put(tableName,new LogRecords_in_range(num_op));
    }


    /**
     * called by the {@link engine.transaction.TxnProcessingEngine} to add logRecord
     * @param myList
     */
    public void addLogRecord(MyList myList){
        if(!holder_by_tableName.get(myList.getTable_name()).hasKey.containsKey(myList.getPrimaryKey())){
            holder_by_tableName.get(myList.getTable_name()).holder_by_range.get(myList.getPartitionId()).add(myList.getLogRecord());
            holder_by_tableName.get(myList.getTable_name()).hasKey.put(myList.getPrimaryKey(), true);
        }
    }
    public UpdateLogWrite asyncCommitLog(long globalLSN, long timestamp, LogStreamFactory logStreamFactory) throws IOException {
        if(enable_parallel){
            List<LogStreamWithResultProvider> providers=LogStreamWithResultProvider.createMultipleStream(logStreamFactory, partitionNum);
            return new ParallelUpdateLogWrite(holder_by_tableName,providers,timestamp,globalLSN);
        }else{
            LogStreamWithResultProvider logStreamWithResultProvider=LogStreamWithResultProvider.createSimpleStream(logStreamFactory);
            return new UpdateLogAsyncWrite(holder_by_tableName,logStreamWithResultProvider,timestamp,globalLSN);
        }
    }
    public boolean undoLog(Database db,List<Integer> rangeId) throws IOException, DatabaseException {
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Map.Entry holder_by_range:logRecordsInRange.holder_by_range.entrySet()){
                if(!rangeId.contains(holder_by_range.getKey())){
                    Vector<LogRecord> logRecords= (Vector<LogRecord>) holder_by_range.getValue();
                    Iterator<LogRecord> logRecordIterator=logRecords.iterator();
                    while (logRecordIterator.hasNext()){
                        LogRecord logRecord =logRecordIterator.next();
                        db.InsertRecord(logRecord.getTableName(), logRecord.getCopyTableRecord());
                    }
                }
            }
        }
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Vector<LogRecord> logRecords:logRecordsInRange.holder_by_range.values()){
                logRecords.clear();
            }
            logRecordsInRange.hasKey.clear();
        }
        return true;
    }
    public void close(){
        writeExecutor.shutdown();
    }
}
