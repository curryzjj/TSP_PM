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
    //<TableName,LogRecords_in_range>
    private ConcurrentHashMap<String,LogRecords_in_range> holder_by_tableName;
    private int partitionNum;
    /* init in the EM */
    public static ExecutorService writeExecutor;
    private long globalLSN;
    public class LogRecords_in_range{
        //<partitionId,<markId,Vector>>
        public ConcurrentHashMap<Integer,ConcurrentHashMap<Long,Vector<LogRecord>>> holder_by_range = new ConcurrentHashMap<>();
        public ConcurrentHashMap<String,Boolean> hasKey;
        public LogRecords_in_range(Integer num_op){
            int i;
            for (i=0;i<num_op;i++){
                ConcurrentHashMap<Long,Vector<LogRecord>> logRecords = new ConcurrentHashMap<>();
                holder_by_range.put(i,logRecords);
                this.hasKey = new ConcurrentHashMap<>();
            }
        }
        public void addUndoLog(long g) {
            globalLSN = g;
            hasKey.clear();
            for (int i:holder_by_range.keySet()){
                holder_by_range.get(i).putIfAbsent(g,new Vector<>());
            }
        }
    }
    public WALManager(int partitionNum){
        this.partitionNum = partitionNum;
        this.holder_by_tableName = new ConcurrentHashMap<>();
    }
    public boolean isEmpty(long offset){
        boolean flag = true;
        for(LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for (ConcurrentHashMap<Long,Vector<LogRecord>> vectors : logRecordsInRange.holder_by_range.values()) {
                if (!vectors.get(offset).isEmpty()) {
                    flag = false;
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
            holder_by_tableName.get(myList.getTable_name()).holder_by_range.get(myList.getPartitionId()).get(globalLSN).add(myList.getLogRecord());
            holder_by_tableName.get(myList.getTable_name()).hasKey.put(myList.getPrimaryKey(), true);
        }
    }
    public UpdateLogWrite asyncCommitLog(long globalLSN, long timestamp, LogStreamFactory logStreamFactory) throws IOException {
        if(enable_parallel){
            List<LogStreamWithResultProvider> providers = LogStreamWithResultProvider.createMultipleStream(logStreamFactory, partitionNum);
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
                    Vector<LogRecord> logRecords = ((ConcurrentHashMap<Long,Vector<LogRecord>>) holder_by_range.getValue()).get(globalLSN);
                    Iterator<LogRecord> logRecordIterator = logRecords.iterator();
                    while (logRecordIterator.hasNext()){
                        LogRecord logRecord =logRecordIterator.next();
                        db.InsertRecord(logRecord.getTableName(), logRecord.getCopyTableRecord());
                    }
                }
            }
        }
        return true;
    }
    public boolean undoLogToAlignOffset(Database db, List<Integer> rangeId, long targetOffset) throws IOException, DatabaseException {
        if (targetOffset < globalLSN) {
            for (WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()) {
                for(Map.Entry holder_by_range:logRecordsInRange.holder_by_range.entrySet()){
                    if(!rangeId.contains(holder_by_range.getKey())){
                        Vector<LogRecord> logRecords = null;
                        for (long offset:((ConcurrentHashMap<Long,Vector<LogRecord>>) holder_by_range.getValue()).keySet()){
                            if (offset > targetOffset) {
                                logRecords = ((ConcurrentHashMap<Long,Vector<LogRecord>>) holder_by_range.getValue()).get(offset);
                                break;
                            }
                        }
                        Iterator<LogRecord> logRecordIterator = logRecords.iterator();
                        while (logRecordIterator.hasNext()){
                            LogRecord logRecord = logRecordIterator.next();
                            db.InsertRecord(logRecord.getTableName(), logRecord.getCopyTableRecord());
                        }
                    }
                }
            }
        }
        return true;
    }
    public boolean cleanUndoLog(long offset) {
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(ConcurrentHashMap<Long, Vector<LogRecord>> logRecords: logRecordsInRange.holder_by_range.values()){
                logRecords.entrySet().removeIf(longVectorEntry -> longVectorEntry.getKey() <= offset);
            }
        }
        return true;
    }
    public void addLogForBatch(long markerId) {
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
           logRecordsInRange.addUndoLog(markerId);
        }
    }
    public void close(){
        writeExecutor.shutdown();
    }
}
