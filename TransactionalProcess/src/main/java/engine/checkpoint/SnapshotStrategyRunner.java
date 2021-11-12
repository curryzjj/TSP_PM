package engine.checkpoint;

import engine.checkpoint.CheckpointStream.CheckpointStreamFactory;
import engine.checkpoint.ShapshotResources.SnapshotResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CloseableRegistry.CloseableRegistry;
import utils.TransactionalProcessConstants.SnapshotExecutionType;

import javax.annotation.Nonnull;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static utils.TransactionalProcessConstants.SnapshotExecutionType.SYNCHRONOUS;

/**
 * A class to execute a {@link SnapshotStrategy}. It can execute a strategy either sync or async.
 * It takes care if common logging and resource cleaning.
 */
public final class SnapshotStrategyRunner<SR extends SnapshotResources> {
    private static final Logger LOG= LoggerFactory.getLogger(SnapshotStrategyRunner.class);
    private static final String LOG_SYNC_COMPLETED_TEMPLATE =
            "{} ({}, synchronous part) in thread {} took {} ms.";
    private static final String LOG_ASYNC_COMPLETED_TEMPLATE =
            "{} ({}, asynchronous part) in thread {} took {} ms.";
    /**
     * Descriptive name of the snapshot strategy that will appear in the log outputs.
     */
    private final String description;
    private final SnapshotStrategy<SR> snapshotStrategy;
    private final SnapshotExecutionType executionType;
    private final CloseableRegistry cancelStreamRegistry;

    public SnapshotStrategyRunner(String description,
                                  SnapshotStrategy<SR> snapshotStrategy,
                                  SnapshotExecutionType executionType,
                                  CloseableRegistry cancelStreamRegistry) {
        this.description = description;
        this.snapshotStrategy = snapshotStrategy;
        this.executionType = executionType;
        this.cancelStreamRegistry = cancelStreamRegistry;
    }
    public final RunnableFuture<SnapshotResult> snapshot(long checkpointId,
                                                         long timestamp,
                                                         @Nonnull CheckpointStreamFactory streamFactory,
                                                         @Nonnull CheckpointOptions checkpointOptions) throws Exception {
        long startTime=System.currentTimeMillis();
        SR snapshotResources=snapshotStrategy.syncPrepareResources(checkpointId);
        SnapshotStrategy.SnapshotResultSupplier asyncSnapshot=
                snapshotStrategy.asyncSnapshot(snapshotResources,
                        checkpointId,
                        timestamp,
                        streamFactory,
                        checkpointOptions);
        FutureTask<SnapshotResult> asyncSnapshotTask=
                new AsyncSnapshotCallable<SnapshotResult>(){
                    @Override
                    protected SnapshotResult callInternal() throws Exception {
                        return asyncSnapshot.get(snapshotCloseableRegistry);
                    }

                    @Override
                    protected void cleanupProvidedResources() {
                        if (snapshotResources != null) {
                            snapshotResources.release();
                        }
                    }
                    @Override
                    protected void logAsyncSnapshotComplete(long startTime) {

                    }
                }.toAsyncSnapshotFutureTask(cancelStreamRegistry);
        if (executionType == SYNCHRONOUS) {
            asyncSnapshotTask.run();
        }

        return asyncSnapshotTask;
    }

    @Override
    public String toString() {
        return "SnapshotStrategy {" + description + "}";
    }
}
