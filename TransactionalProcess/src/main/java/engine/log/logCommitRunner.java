package engine.log;

import engine.log.LogStream.LogStreamFactory;
import engine.log.LogStream.UpdateLogAsyncWrite;
import engine.log.LogStream.UpdateLogWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CloseableRegistry.CloseableRegistry;
import utils.TransactionalProcessConstants.CommitLogExecutionType;

import java.io.IOException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static utils.TransactionalProcessConstants.CommitLogExecutionType.SYNCHRONOUS;

public final class logCommitRunner {
    private static final Logger LOG= LoggerFactory.getLogger(logCommitRunner.class);
    private final CloseableRegistry cancelStreamRegistry;
    private final WALManager walManager;
    private final CommitLogExecutionType executionType;

    public logCommitRunner(CloseableRegistry cancelStreamRegistry,
                           WALManager walManager,
                           CommitLogExecutionType type) {
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.walManager=walManager;
        executionType = type;
    }
    public final RunnableFuture<LogResult> commitLog(long globalLSN, long timestamp, LogStreamFactory logStreamFactory) throws IOException {
        long startTime=System.currentTimeMillis();
        if (walManager.isEmpty()){
            LOG.info("There is no update log to commit");
            return null;
        }
        UpdateLogWrite updateWrite=walManager.asyncCommitLog(globalLSN,timestamp,logStreamFactory);
        FutureTask<LogResult> asyncCommitLogTask= new AsyncLogCommitCallable<LogResult>() {
            @Override
            protected LogResult callInternal() throws Exception {
                return updateWrite.get(cancelStreamRegistry);
            }

            @Override
            protected void cleanupProvidedResources() {

            }
        }.toAsyncLogCommitFutureTask(cancelStreamRegistry);
        if (executionType==SYNCHRONOUS){
            asyncCommitLogTask.run();
        }
        return asyncCommitLogTask;
    }
}
