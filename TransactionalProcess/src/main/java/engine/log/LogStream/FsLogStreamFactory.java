package engine.log.LogStream;

import System.FileSystem.FSDataOutputStream;
import System.FileSystem.FileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class FsLogStreamFactory implements LogStreamFactory {
    private static final Logger LOG= LoggerFactory.getLogger(LogStreamFactory.class);
    /** Maximum size of state that is stored with the metadata, rather than in files. */
    public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
    private final int writeBufferSize;

    /** State below this size will be stored as part of the metadata, rather than in files. */
    private final int fileThreshold;

    /** The directory for shapshot exclusive state data. */
    private final Path LogDirectory;

    /** Cached handle to the file system for file operations. */
    private final FileSystem filesystem;

    public FsLogStreamFactory(int writeBufferSize,
                              int fileThreshold,
                              Path logDirectory,
                              FileSystem filesystem) {
        if (fileThreshold < 0) {
            throw new IllegalArgumentException(
                    "The threshold for file state size must be zero or larger.");
        }

        if (writeBufferSize < 0) {
            throw new IllegalArgumentException("The write buffer size must be zero or larger.");
        }

        if (fileThreshold > MAX_FILE_STATE_THRESHOLD) {
            throw new IllegalArgumentException(
                    "The threshold for file state size cannot be larger than "
                            + MAX_FILE_STATE_THRESHOLD);
        }
        this.writeBufferSize = writeBufferSize;
        this.fileThreshold = fileThreshold;
        LogDirectory = logDirectory;
        this.filesystem = filesystem;
    }

    @Override
    public String toString() {
        return "FsLogStreamFactory @ "+LogDirectory;
    }
    @Override
    public LogOutputStream createLogOutputStream() throws IOException {
        Path target=LogDirectory;
        int bufferSize=Math.max(writeBufferSize,fileThreshold);
        return new FsLogOutputStream(bufferSize,fileThreshold,target,filesystem);
    }
    public static class FsLogOutputStream extends LogOutputStream{
        private final byte[] writeBuffer;
        private int pos;
        private FSDataOutputStream outStream;
        private final int localThreshold;
        private final Path basePath;
        private final FileSystem fs;
        private Path logPath;
        private String relativeLogPath;
        private volatile boolean closed;
        private File logFile;

        public FsLogOutputStream( int bufferSize, int localThreshold, Path basePath, FileSystem fs) {
            if (bufferSize < localThreshold) {
                throw new IllegalArgumentException();
            }
            this.writeBuffer = new byte[bufferSize];
            this.localThreshold = localThreshold;
            this.basePath = basePath;
            this.fs = fs;
            this.logPath=createLogPath();
            this.logFile=fs.pathToFile(logPath);
        }

        @Override
        public long getPos() throws IOException {
            return pos + (outStream == null ? 0 : outStream.getPos());
        }

        @Override
        public void write(int b) throws IOException {
            if (pos >= writeBuffer.length) {
            }
            writeBuffer[pos++] = (byte) b;
            flushToFile();
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
            if (outStream != null || pos > localThreshold) {
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
                        LOG.warn("Could not close the state stream for {}.", logPath, throwable);
                    } finally {
                        try {
                            fs.delete(logPath, false);
                        } catch (Exception e) {
                            LOG.warn(
                                    "Cannot delete closed and discarded state stream for {}.",
                                    logPath,
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
        private Path createLogPath() {
            final String fileName = "WAL";
            relativeLogPath = fileName;
            return new Path(basePath, fileName);
        }

        private void createStream() throws IOException {
            Exception latestException = null;
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    this.outStream =new LocalDataOutputStream(logFile);
                    return;
                } catch (Exception e) {
                    latestException = e;
                }
            }

            throw new IOException(
                    "Could not open output stream for WAL", latestException);
        }
        @Override
        public Path getLogPath() {
            return logPath;
        }
    }
}
