package engine.log.LogStream;

import System.FileSystem.DataIO.DataOutputView;
import System.FileSystem.DataIO.DataOutputViewStreamWrapper;
import engine.log.LogRecord;
import engine.log.LogResult;
import engine.log.WALManager;
import engine.table.datatype.serialize.Serialize;
import utils.CloseableRegistry.CloseableRegistry;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static engine.log.WALManager.writeExecutor;
import static utils.TransactionalProcessConstants.FaultTolerance.END_OF_GLOBAL_LSN_MARK;

public class ParallelUpdateLogWrite implements UpdateLogWrite {
    private final List<LogStreamWithResultProvider> providers;
    private final long timestamp;
    private final long globalLSN;
    private ConcurrentHashMap<String, WALManager.LogRecords_in_range> holder_by_tableName;
    public ParallelUpdateLogWrite(ConcurrentHashMap<String, WALManager.LogRecords_in_range> holder_by_tableName,
                                  List<LogStreamWithResultProvider> providers,
                                  long timestamp,
                                  long globalLSN) {
        this.providers = providers;
        this.timestamp = timestamp;
        this.globalLSN = globalLSN;
        this.holder_by_tableName = holder_by_tableName;
    }

    @Override
    public LogResult get(CloseableRegistry logCloseableRegistry) throws Exception {
        List<CommitLogTask> callables=new ArrayList<>();
        initTasks(callables,logCloseableRegistry);
        initIterator(callables);
        List<Future<Boolean>> futures=writeExecutor.invokeAll(callables);
        for(WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(Vector<LogRecord> logRecords:logRecordsInRange.holder_by_range.values()){
                logRecords.clear();
            }
        }
        return new LogResult();
    }

    private void initIterator(List<CommitLogTask> callables) {
        for (WALManager.LogRecords_in_range logRecordsInRange:holder_by_tableName.values()){
            for(int i=0;i<logRecordsInRange.holder_by_range.size();i++){
                Iterator<LogRecord> logs=logRecordsInRange.holder_by_range.get(i).iterator();
                callables.get(i).setIterators(logs);
            }
        }
    }

    private void initTasks(List<CommitLogTask> callables,CloseableRegistry logCloseableRegistry){
        for(int i=0;i<providers.size();i++){
            List<Iterator> iterators=new ArrayList<>();
            callables.add(new CommitLogTask(iterators,providers.get(i),logCloseableRegistry));
        }
    }
    private class CommitLogTask implements Callable<Boolean> {
        private List<Iterator> iterators;
        private LogStreamWithResultProvider provider;
        private CloseableRegistry logCloseableRegistry;
        public CommitLogTask(List<Iterator> iterators, LogStreamWithResultProvider provider,CloseableRegistry logCloseableRegistry){
            this.iterators =iterators;
            this.provider=provider;
            this.logCloseableRegistry=logCloseableRegistry;
        }

        public void setIterators(Iterator<LogRecord> iterator) {
            this.iterators.add(iterator);
        }

        @Override
        public Boolean call() throws Exception {
            logCloseableRegistry.registerCloseable(provider);
            final DataOutputView outputView=new DataOutputViewStreamWrapper(provider.getLogOutputStream());
            for(int i=0;i<iterators.size();i++){
                Iterator<LogRecord> logs=iterators.get(i);
                while (logs.hasNext()){
                    LogRecord logRecord = logs.next();
                    writeLogRecord(outputView,logRecord);
                }
            }
            outputView.writeInt(END_OF_GLOBAL_LSN_MARK);
            outputView.writeLong(globalLSN);
            provider.getLogOutputStream().flush();
            if(logCloseableRegistry.unregisterCloseable(provider)){
                provider.closeAndFinalizeLogCommitStreamResult();
            }else{
                throw new IOException("Stream is already unregistered/closed.");
            }
            return true;
        }
    }
    private void writeLogRecord(DataOutputView outputView,LogRecord logRecord) throws IOException {
        byte[] serializeObject= Serialize.serializeObject(logRecord);
        int len=serializeObject.length;
        outputView.writeInt(len);
        outputView.write(serializeObject);
    }
}
