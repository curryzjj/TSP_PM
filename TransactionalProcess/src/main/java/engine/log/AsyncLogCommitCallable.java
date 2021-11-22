package engine.log;

import engine.shapshot.AsyncSnapshotCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CloseableRegistry.CloseableRegistry;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AsyncLogCommitCallable<T> implements Callable {
    private static final Logger LOG= LoggerFactory.getLogger(AsyncLogCommitCallable.class);
    /** This is used to atomically claim ownership for the resource cleanup. */
    @Nonnull
    private final AtomicBoolean resourceCleanupOwnershipTaken;
    /**
     * Registers streams that can block in I/O during commit in-memory log. Forwards close from
     * taskCancelCloseableRegistry.
     */
    @Nonnull protected final CloseableRegistry logCloseableRegistry;

    protected AsyncLogCommitCallable() {
        resourceCleanupOwnershipTaken = new AtomicBoolean(false);
        logCloseableRegistry = new CloseableRegistry();
    }

    @Override
    public T call() throws Exception {
        final long startTime=System.currentTimeMillis();
        if(resourceCleanupOwnershipTaken.compareAndSet(false,true)){
            try {
                T result = callInternal();
                logAsyncComplete(startTime);
                return result;
            } catch (Exception ex) {
                if (!logCloseableRegistry.isClosed()) {
                    throw ex;
                }
            } finally {
                closelogIO();
                cleanup();
            }
        }
        return null;
    }
    /**
     * This method implements the (async) log commit logic. Resources acquired within this method should be released at the end of the method.
     * @return
     * @throws Exception
     */
    protected abstract T callInternal() throws Exception;

    protected abstract void cleanupProvidedResources();
    private void cleanup() {
        cleanupProvidedResources();
    }
    /**
     * This method is invoked after completion of the log commit and can be
     * overridden to output a logging about the duration of the async part.
     */
    protected void logAsyncComplete(long startTime) {

    }
    private void closelogIO() {
        try {
            logCloseableRegistry.close();
        } catch (IOException e) {
            LOG.warn("Could not properly close log commit streams.", e);
        }
    }
    public AsyncLogCommitCallable.AsyncLogCommitTask toAsyncLogCommitFutureTask(CloseableRegistry closeableRegistry) throws IOException {
        return new AsyncLogCommitTask(closeableRegistry);
    }
    /**
     * {@link java.util.concurrent.FutureTask} that wraps a {@link AsyncSnapshotCallable} and
     * connects it with cancellation and closing.
     */
    public class AsyncLogCommitTask extends FutureTask {
        @Nonnull private final CloseableRegistry taskRegistry;

        @Nonnull private final Closeable cancelOnClose;

        private AsyncLogCommitTask(@Nonnull CloseableRegistry taskRegistry) throws IOException {
            super(AsyncLogCommitCallable.this);
            this.cancelOnClose = () -> cancel(true);
            this.taskRegistry = taskRegistry;
            taskRegistry.registerCloseable(cancelOnClose);
        }
        @Override
        protected void done() {
            super.done();
            taskRegistry.unregisterCloseable(cancelOnClose);
        }
    }
}
