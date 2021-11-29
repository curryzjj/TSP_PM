package engine.log;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.log.LogStream.*;
import engine.transaction.common.MyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static UserApplications.CONTROL.enable_parallel;

public class WALManager {
    private static final Logger LOG= LoggerFactory.getLogger(WALManager.class);
    private static final String DESCRIPTION="Asynchronous commit update log";
    private ConcurrentHashMap<String,LogRecords_in_range> holder_by_tableName;
    private int rangeNum;
    public static ExecutorService writeExecutor;
    public class LogRecords_in_range{
        public ConcurrentHashMap<Integer, Vector<LogRecord>> holder_by_range=new ConcurrentHashMap<>();
        public LogRecords_in_range(Integer num_op){
            int i;
            for (i=0;i<num_op;i++){
                holder_by_range.put(i,new Vector<>());
            }
        }
    }
    public WALManager(int num_op){
        this.rangeNum=num_op;
        this.holder_by_tableName=new ConcurrentHashMap<>();
        writeExecutor=Executors.newFixedThreadPool(num_op);
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
        holder_by_tableName.get(myList.getTable_name()).holder_by_range.get(myList.getRange()).add(myList.getLogRecord());
    }
    public UpdateLogWrite asyncCommitLog(long globalLSN, long timestamp, LogStreamFactory logStreamFactory) throws IOException {
        if(enable_parallel){
            List<LogStreamWithResultProvider> providers=LogStreamWithResultProvider.createMultipleStream(logStreamFactory,rangeNum);
            return new ParallelUpdateLogWrite(holder_by_tableName,providers,timestamp,globalLSN);
        }else{
            LogStreamWithResultProvider logStreamWithResultProvider=LogStreamWithResultProvider.createSimpleStream(logStreamFactory);
            return new UpdateLogAsyncWrite(holder_by_tableName,logStreamWithResultProvider,timestamp,globalLSN);
        }
    }
    public boolean undoLog(Database db) throws IOException, DatabaseException {
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Vector<LogRecord> logRecords:logRecordsInRange.holder_by_range.values()){
                Iterator<LogRecord> logRecordIterator=logRecords.iterator();
                while (logRecordIterator.hasNext()){
                    LogRecord logRecord =logRecordIterator.next();
                    db.InsertRecord(logRecord.getTableName(), logRecord.getCopyTableRecord());
                }
            }
        }
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Vector<LogRecord> logRecords:logRecordsInRange.holder_by_range.values()){
                logRecords.clear();
            }
        }
        return true;
    }
    public void close(){
        writeExecutor.shutdown();
    }
}
