package engine.checkpoint.CheckpointStream;

import System.FileSystem.FSDataOutputStream;
import System.FileSystem.FileSystem;
import System.FileSystem.Path;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class FsCheckpointStreamFactory implements CheckpointStreamFactory{
    private static final Logger LOG= LoggerFactory.getLogger(CheckpointStreamFactory.class);
    /** Maximum size of state that is stored with the metadata, rather than in files. */
    public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
    private final int writeBufferSize;

    /** State below this size will be stored as part of the metadata, rather than in files. */
    private final int fileStateThreshold;

    /** The directory for checkpoint exclusive state data. */
    private final Path checkpointDirectory;

    /** The directory for shared checkpoint data. */
    private final Path sharedStateDirectory;

    /** Cached handle to the file system for file operations. */
    private final FileSystem filesystem;

    /** Whether the file system dynamically injects entropy into the file paths. */
    private final boolean entropyInjecting;

    public FsCheckpointStreamFactory(int writeBufferSize,
                                     int fileStateSizeThreshold,
                                     Path checkpointDirectory,
                                     Path sharedStateDirectory,
                                     FileSystem filesystem,
                                     boolean entropyInjecting) {
        if (fileStateSizeThreshold < 0) {
            throw new IllegalArgumentException(
                    "The threshold for file state size must be zero or larger.");
        }

        if (writeBufferSize < 0) {
            throw new IllegalArgumentException("The write buffer size must be zero or larger.");
        }

        if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
            throw new IllegalArgumentException(
                    "The threshold for file state size cannot be larger than "
                            + MAX_FILE_STATE_THRESHOLD);
        }
        this.writeBufferSize = writeBufferSize;
        this.fileStateThreshold = fileStateSizeThreshold;
        this.checkpointDirectory = checkpointDirectory;
        this.sharedStateDirectory = sharedStateDirectory;
        this.filesystem = filesystem;
        this.entropyInjecting = entropyInjecting;
    }
    @Override
    public String toString() {
        return "File Stream Factory @ " + checkpointDirectory;
    }
    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream() throws IOException {
        Path target=checkpointDirectory;
        int bufferSize=Math.max(writeBufferSize,fileStateThreshold);
        final boolean absolutePath=true;
        return new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                target, filesystem, bufferSize, fileStateThreshold, !absolutePath);
    }
    public static class FsCheckpointStateOutputStream extends CheckpointStreamFactory.CheckpointStateOutputStream{
        private final byte[] writeBuffer;
        private int pos;
        private FSDataOutputStream outStream;
        private final int localStateThreshold;
        private final Path basePath;
        private final FileSystem fs;
        private Path statePath;
        private String relativeStatePath;
        private volatile boolean closed;
        private final boolean allowRelativePaths;
        public FsCheckpointStateOutputStream(
                Path basePath, FileSystem fs, int bufferSize, int localStateThreshold) {
            this(basePath, fs, bufferSize, localStateThreshold, false);
        }
        public FsCheckpointStateOutputStream(
                Path basePath,
                FileSystem fs,
                int bufferSize,
                int localStateThreshold,
                boolean allowRelativePaths) {

            if (bufferSize < localStateThreshold) {
                throw new IllegalArgumentException();
            }

            this.basePath = basePath;
            this.fs = fs;
            this.writeBuffer = new byte[bufferSize];
            this.localStateThreshold = localStateThreshold;
            this.allowRelativePaths = allowRelativePaths;
        }

        @Override
        public long getPos() throws IOException {
            return pos + (outStream == null ? 0 : outStream.getPos());
        }

        @Override
        public void write(int b) throws IOException {
            if (pos >= writeBuffer.length) {
                flushToFile();
            }
            writeBuffer[pos++] = (byte) b;
        }

        @Override
        public void write(@NotNull byte[] b, int off, int len) throws IOException {
            if (len < writeBuffer.length) {
                // copy it into our write buffer first
                final int remaining = writeBuffer.length - pos;
                if (len > remaining) {
                    // copy as much as fits
                    System.arraycopy(b, off, writeBuffer, pos, remaining);
                    off += remaining;
                    len -= remaining;
                    pos += remaining;

                    // flushToFile the write buffer to make it clear again
                    flushToFile();
                }

                // copy what is in the buffer
                System.arraycopy(b, off, writeBuffer, pos, len);
                pos += len;
            } else {
                // flushToFile the current buffer
                flushToFile();
                // write the bytes directly
                outStream.write(b, off, len);
            }
        }

        @Override
        public void flush() throws IOException {
            if (outStream != null || pos > localStateThreshold) {
                flushToFile();
            }
        }

        @Override
        public void sync() throws IOException {
            outStream.sync();
        }

        /**
         * Checks whether the stream is closed.
         *
         * @return True if the stream was closed, false if it is still open.
         */
        public boolean isClosed() {
            return closed;
        }
        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                // make sure write requests need to go to 'flushToFile()' where they recognized
                // that the stream is closed
                pos = writeBuffer.length;

                if (outStream != null) {
                    try {
                        outStream.close();
                    } catch (Throwable throwable) {
                        LOG.warn("Could not close the state stream for {}.", statePath, throwable);
                    } finally {
                        try {
                            fs.delete(statePath, false);
                        } catch (Exception e) {
                            LOG.warn(
                                    "Cannot delete closed and discarded state stream for {}.",
                                    statePath,
                                    e);
                        }
                    }
                }
            }
        }
        public void flushToFile() throws IOException {
            if (!closed) {
                // initialize stream if this is the first flushToFile (stream flush, not Darjeeling
                // harvest)
                if (outStream == null) {
                    createStream();
                }

                if (pos > 0) {
                    outStream.write(writeBuffer, 0, pos);
                    pos = 0;
                }
            } else {
                throw new IOException("closed");
            }
        }
        private Path createStatePath() {
            final String fileName = UUID.randomUUID().toString();
            relativeStatePath = fileName;
            return new Path(basePath, fileName);
        }

        private void createStream() throws IOException {
            Exception latestException = null;
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    this.outStream =fs.create(createStatePath());
                    this.statePath = createStatePath();
                    return;
                } catch (Exception e) {
                    latestException = e;
                }
            }

            throw new IOException(
                    "Could not open output stream for state backend", latestException);
        }
    }
}
