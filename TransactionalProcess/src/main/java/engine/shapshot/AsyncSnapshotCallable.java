package engine.shapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CloseableRegistry.CloseableRegistry;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AsyncSnapshotCallable<T> implements Callable {
    /** Message for the cancellation of the shapshot*/
    private static final String CANCELLATION_EXECEPTION_MSG="Async snapshot was cancelled";
    private static final Logger LOG= LoggerFactory.getLogger(AsyncSnapshotCallable.class);
    /** This is used to atomically claim ownership for the resource cleanup. */
    @Nonnull
    private final AtomicBoolean resourceCleanupOwnershipTaken;
    /**
     * Registers streams that can block in I/O during snapshot. Forwards close from
     * taskCancelCloseableRegistry.
     */
    @Nonnull protected final CloseableRegistry snapshotCloseableRegistry;

    protected AsyncSnapshotCallable() {
        snapshotCloseableRegistry = new CloseableRegistry();
        resourceCleanupOwnershipTaken = new AtomicBoolean(false);
    }

    /**
     * Creates a future task from this and registers it with the given {@link CloseableRegistry}.
     * The task is unregistered again in {@link FutureTask}.
     */
    public AsyncSnapshotCallable.AsyncSnapshotTask toAsyncSnapshotFutureTask(@Nonnull CloseableRegistry taskRegistry)
            throws IOException {
        return new AsyncSnapshotCallable.AsyncSnapshotTask(taskRegistry);
    }

    @Override
    public T call() throws Exception {
        final long startTime=System.currentTimeMillis();
        if(resourceCleanupOwnershipTaken.compareAndSet(false,true)){
            try {
                T result = callInternal();
                logAsyncSnapshotComplete(startTime);
                return result;
            } catch (Exception ex) {
                if (!snapshotCloseableRegistry.isClosed()) {
                    throw ex;
                }
            } finally {
                closeSnapshotIO();
                cleanup();
            }
        }
        return null;
    }

    /**
     * This method implements the (async) snapshot logic. Resources acquired within this method should be released at the end of the method.
     * @return
     * @throws Exception
     */
    protected abstract T callInternal() throws Exception;

    /**
     * This method is invoked after completion of the snapshot and can be
     * overridden to output a logging about the duration of the async part.
     */
    protected abstract void cleanupProvidedResources();
    private void cleanup() {
        cleanupProvidedResources();
    }
    /**
     * This method is invoked after completion of the snapshot and can be overridden to output a
     * logging about the duration of the async part.
     */
    protected void logAsyncSnapshotComplete(long startTime) {}
    private void closeSnapshotIO() {
        try {
            snapshotCloseableRegistry.close();
        } catch (IOException e) {
            LOG.warn("Could not properly close incremental snapshot streams.", e);
        }
    }

    /**
     * {@link java.util.concurrent.FutureTask} that wraps a {@link AsyncSnapshotCallable} and
     * connects it with cancellation and closing.
     */
    public class AsyncSnapshotTask extends FutureTask{
        @Nonnull private final CloseableRegistry taskRegistry;

        @Nonnull private final Closeable cancelOnClose;

        private AsyncSnapshotTask(@Nonnull CloseableRegistry taskRegistry) throws IOException {
            super(AsyncSnapshotCallable.this);
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
