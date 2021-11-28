package engine.log.LogStream;

import System.FileSystem.FSDataOutputStream;
import System.FileSystem.Path;

import javax.annotation.Nullable;
import java.io.IOException;

public interface LogStreamFactory {
    LogOutputStream createLogOutputStream(int rangeId) throws IOException;
    @Nullable
    abstract class LogOutputStream extends FSDataOutputStream{
        public abstract Path getLogPath();
    }
}
