package System.util;

import org.slf4j.Logger;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;

public final class IOUtil {
        private static final int BLOCKSIZE = 4096;

        public static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close) throws IOException {
            PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
            byte[] buf = new byte[buffSize];

            try {
                for(int bytesRead = in.read(buf); bytesRead >= 0; bytesRead = in.read(buf)) {
                    out.write(buf, 0, bytesRead);
                    if (ps != null && ps.checkError()) {
                        throw new IOException("Unable to write to output stream.");
                    }
                }
            } finally {
                if (close) {
                    out.close();
                    in.close();
                }

            }

        }

        public static void copyBytes(InputStream in, OutputStream out) throws IOException {
            copyBytes(in, out, 4096, true);
        }

        public static void copyBytes(InputStream in, OutputStream out, boolean close) throws IOException {
            copyBytes(in, out, 4096, close);
        }

        public static void readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
            int ret;
            for(int toRead = len; toRead > 0; off += ret) {
                ret = in.read(buf, off, toRead);
                if (ret < 0) {
                    throw new IOException("Premeture EOF from inputStream");
                }

                toRead -= ret;
            }

        }

        public static int tryReadFully(InputStream in, byte[] buf) throws IOException {
            int totalRead;
            int read;
            for(totalRead = 0; totalRead != buf.length; totalRead += read) {
                read = in.read(buf, totalRead, buf.length - totalRead);
                if (read == -1) {
                    break;
                }
            }

            return totalRead;
        }

        public static void skipFully(InputStream in, long len) throws IOException {
            while(len > 0L) {
                long ret = in.skip(len);
                if (ret < 0L) {
                    throw new IOException("Premeture EOF from inputStream");
                }

                len -= ret;
            }

        }

        public static void cleanup(Logger log, AutoCloseable... closeables) {
            AutoCloseable[] var2 = closeables;
            int var3 = closeables.length;

            for(int var4 = 0; var4 < var3; ++var4) {
                AutoCloseable c = var2[var4];
                if (c != null) {
                    try {
                        c.close();
                    } catch (Exception var7) {
                        if (log != null && log.isDebugEnabled()) {
                            log.debug("Exception in closing " + c, var7);
                        }
                    }
                }
            }

        }

        public static void closeStream(Closeable stream) {
            cleanup((Logger)null, stream);
        }

        public static void closeSocket(Socket sock) {
            if (sock != null) {
                try {
                    sock.close();
                } catch (IOException var2) {
                }
            }

        }

        public static void closeAll(AutoCloseable... closeables) throws Exception {
            closeAll((Iterable) Arrays.asList(closeables));
        }

        public static void closeAll(Iterable<? extends AutoCloseable> closeables) throws Exception {
            if (null != closeables) {
                Exception collectedExceptions = null;
                Iterator var2 = closeables.iterator();

                while(var2.hasNext()) {
                    AutoCloseable closeable = (AutoCloseable)var2.next();

                    try {
                        if (null != closeable) {
                            closeable.close();
                        }
                    } catch (Exception var5) {

                    }
                }

                if (null != collectedExceptions) {
                    throw collectedExceptions;
                }
            }

        }

        public static void closeAllQuietly(AutoCloseable... closeables) {
            closeAllQuietly((Iterable)Arrays.asList(closeables));
        }

        public static void closeAllQuietly(Iterable<? extends AutoCloseable> closeables) {
            if (null != closeables) {
                Iterator var1 = closeables.iterator();

                while(var1.hasNext()) {
                    AutoCloseable closeable = (AutoCloseable)var1.next();
                    closeQuietly(closeable);
                }
            }

        }

        public static void closeQuietly(AutoCloseable closeable) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Throwable var2) {
            }

        }

        public static void deleteFileQuietly(Path path) {
            try {
                Files.deleteIfExists(path);
            } catch (Throwable var2) {
            }

        }

        private IOUtil() {
        }
    }
