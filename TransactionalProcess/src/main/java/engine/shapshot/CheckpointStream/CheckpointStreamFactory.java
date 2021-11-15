package engine.shapshot.CheckpointStream;

import System.FileSystem.FSDataOutputStream;

import java.io.IOException;

public interface CheckpointStreamFactory {
    CheckpointStateOutputStream createCheckpointStateOutputStream() throws IOException;
    abstract class CheckpointStateOutputStream extends FSDataOutputStream {

    }
}
