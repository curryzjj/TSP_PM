import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingCheckpointOutputStream extends CheckpointStreamFactory.CheckpointStateOutputStream {
    /** Optional delegate stream to which all ops are forwarded. */
    private final FSDataOutputStream delegate;

    /**
     * Optional latch on which the stream blocks, e.g. until the test triggers it after some call to
     * #close().
     */
    private final OneShotLatch triggerUnblock;

    /**
     * Optional latch on which the test can block until the stream is blocked at the desired
     * blocking position.
     */
    private final OneShotLatch waitForBlocking;

    /** Closed flag. */
    private final AtomicBoolean closed;

    /** The read position at which this will block. 0 by default. */
    private final long blockAtPosition;

    /** The current read position. */
    private long position;

    public BlockingCheckpointOutputStream(
            @Nullable OneShotLatch waitForBlocking, @Nullable OneShotLatch triggerUnblock) {
        this(null, waitForBlocking, triggerUnblock, 0L);
    }

    public BlockingCheckpointOutputStream(
            @Nullable FSDataOutputStream delegate,
            @Nullable OneShotLatch waitForBlock,
            @Nullable OneShotLatch triggerUnblock) {
        this(delegate, waitForBlock, triggerUnblock, 0L);
    }

    public BlockingCheckpointOutputStream(
            @Nullable FSDataOutputStream delegate,
            @Nullable OneShotLatch waitForBlocking,
            @Nullable OneShotLatch triggerUnblock,
            long blockAtPosition) {

        this.delegate = delegate;
        this.triggerUnblock = triggerUnblock;
        this.waitForBlocking = waitForBlocking;
        this.blockAtPosition = blockAtPosition;
        if (delegate != null) {
            try {
                this.position = delegate.getPos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            this.position = 0;
        }
        this.closed = new AtomicBoolean(false);
    }

    @Override
    public void write(int b) throws IOException {

        if (position == blockAtPosition) {
            unblockWaiter();
            awaitUnblocker();
        }

        if (delegate != null) {
            try {
                delegate.write(b);
            } catch (IOException ex) {
                unblockWaiter();
                throw ex;
            }
        }

        // We also check for close here, in case the underlying stream does not do this
        if (closed.get()) {
            throw new IOException("Stream closed.");
        }

        ++position;
    }

    // We override this to ensure that writes go through the blocking #write(int) method!
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        for (int i = 0; i < len; i++) {
            write(b[off + i]);
        }
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void flush() throws IOException {
        if (delegate != null) {
            delegate.flush();
        }
    }

    @Override
    public void sync() throws IOException {
        if (delegate != null) {
            delegate.sync();
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (delegate != null) {
                IOUtils.closeQuietly(delegate);
            }
            // trigger all the latches, essentially all blocking ops on the stream should resume
            // after close.
            unblockAll();
        }
    }

    private void unblockWaiter() {
        if (null != waitForBlocking) {
            waitForBlocking.trigger();
        }
    }

    private void awaitUnblocker() {
        if (null != triggerUnblock) {
            try {
                triggerUnblock.await();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void unblockAll() {
        if (null != waitForBlocking) {
            waitForBlocking.trigger();
        }
        if (null != triggerUnblock) {
            triggerUnblock.trigger();
        }
    }

    @Nullable
    @Override
    public StreamStateHandle closeAndGetHandle() throws IOException {

        if (!closed.compareAndSet(false, true)) {
            throw new IOException("Stream was already closed!");
        }

        if (delegate instanceof CheckpointStreamFactory.CheckpointStateOutputStream) {
            StreamStateHandle streamStateHandle =
                    ((CheckpointStreamFactory.CheckpointStateOutputStream) delegate)
                            .closeAndGetHandle();
            unblockAll();
            return streamStateHandle;
        } else {
            unblockAll();
            throw new IOException("Delegate is not a CheckpointStateOutputStream!");
        }
    }

    public boolean isClosed() {
        return closed.get();
    }
}
