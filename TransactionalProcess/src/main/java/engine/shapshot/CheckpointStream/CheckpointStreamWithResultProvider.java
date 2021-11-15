package engine.shapshot.CheckpointStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public interface CheckpointStreamWithResultProvider extends Closeable {
    Logger LOG= LoggerFactory.getLogger(CheckpointStreamFactory.class);
    CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream();
    default void close() throws IOException{
        getCheckpointOutputStream().close();
    }
    class PrimaryStreamOnly implements CheckpointStreamWithResultProvider{
        private final CheckpointStreamFactory.CheckpointStateOutputStream outputStream;

        public PrimaryStreamOnly(CheckpointStreamFactory.CheckpointStateOutputStream outputStream) {
            this.outputStream = outputStream;
        }
        @Override
        public CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream() {
            return outputStream;
        }
    }
    static CheckpointStreamWithResultProvider createSimpleStream(CheckpointStreamFactory checkpointStreamFactory) throws IOException {
        CheckpointStreamFactory.CheckpointStateOutputStream out=checkpointStreamFactory.createCheckpointStateOutputStream();
        return new PrimaryStreamOnly(out);
    }
}
