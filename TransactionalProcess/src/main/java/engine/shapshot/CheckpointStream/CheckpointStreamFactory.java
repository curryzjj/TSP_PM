package engine.shapshot.CheckpointStream;

import System.FileSystem.FSDataOutputStream;
import System.FileSystem.Path;

import javax.annotation.Nullable;
import java.io.IOException;

public interface CheckpointStreamFactory {
    CheckpointStateOutputStream createCheckpointStateOutputStream() throws IOException;
    @Nullable
    abstract class CheckpointStateOutputStream extends FSDataOutputStream {
        public abstract Path getStatePath();
    }
}
