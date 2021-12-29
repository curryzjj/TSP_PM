package engine.log.LogStream;

import System.FileSystem.DataIO.DataOutputView;
import System.FileSystem.DataIO.DataOutputViewStreamWrapper;
import engine.log.LogRecord;
import engine.log.LogResult;
import engine.log.WALManager;
import engine.table.datatype.serialize.Serialize;
import utils.CloseableRegistry.CloseableRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static utils.TransactionalProcessConstants.FaultTolerance.END_OF_GLOBAL_LSN_MARK;

public class UpdateLogAsyncWrite implements UpdateLogWrite {
    private final LogStreamWithResultProvider logStreamWithResultProvider;
    private final long timestamp;
    private final long globalLSN;
    private ConcurrentHashMap<String, WALManager.LogRecords_in_range> holder_by_tableName;
    public UpdateLogAsyncWrite(ConcurrentHashMap<String, WALManager.LogRecords_in_range> holder_by_tableName,
                               LogStreamWithResultProvider logStreamWithResultProvider,
                               long timestamp,
                               long globalLSN) {
        this.logStreamWithResultProvider = logStreamWithResultProvider;
        this.timestamp = timestamp;
        this.globalLSN = globalLSN;
        this.holder_by_tableName = holder_by_tableName;
    }
    public LogResult get(CloseableRegistry logCloseableRegistry) throws Exception{
        logCloseableRegistry.registerCloseable(logStreamWithResultProvider);
        commitLog();
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Vector<LogRecord> logRecords:logRecordsInRange.holder_by_range.values()){
              logRecords.clear();
            }
            logRecordsInRange.hasKey.clear();
        }
        if(logCloseableRegistry.unregisterCloseable(logStreamWithResultProvider)){
            logStreamWithResultProvider.closeAndFinalizeLogCommitStreamResult();
            return new LogResult();
        }else{
            throw new IOException("Stream is already unregistered/closed.");
        }
    }
    private void commitLog() throws IOException {
        final DataOutputView outputView=new DataOutputViewStreamWrapper(logStreamWithResultProvider.getLogOutputStream());
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Vector<LogRecord> logRecords:logRecordsInRange.holder_by_range.values()){
                Iterator<LogRecord> logRecordIterator=logRecords.iterator();
                while (logRecordIterator.hasNext()){
                    LogRecord logRecord =logRecordIterator.next();
                    writeLogRecord(outputView,logRecord);
                }
            }
        }
        outputView.writeInt(END_OF_GLOBAL_LSN_MARK);
        outputView.writeLong(globalLSN);
        logStreamWithResultProvider.getLogOutputStream().flush();
    }
    private void writeLogRecord(DataOutputView outputView,LogRecord logRecord) throws IOException {
//        if(logRecord.getUpdateTableRecord()!=null){
//            byte[] serializeObject=Serialize.serializeObject(logRecord);
//            int len=serializeObject.length;
//            outputView.writeInt(len);
//            outputView.write(serializeObject);
//        }
        if (logRecord.getUpdateTableRecord()!=null){
            String str=logRecord.toSerializableString();
            outputView.writeInt(str.getBytes(StandardCharsets.UTF_8).length);
            outputView.write(str.getBytes(StandardCharsets.UTF_8));
        }
    }
}
