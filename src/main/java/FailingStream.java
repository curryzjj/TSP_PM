import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nullable;
import java.io.IOException;

public class FailingStream extends CheckpointStreamFactory.CheckpointStateOutputStream {
    private final IOException testException;

    FailingStream(IOException testException) {
        this.testException = testException;
    }

    @Nullable
    @Override
    public StreamStateHandle closeAndGetHandle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPos() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(int b) throws IOException {
        throw testException;
    }

    @Override
    public void flush() throws IOException {
        throw testException;
    }

    @Override
    public void sync() throws IOException {
        throw testException;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
