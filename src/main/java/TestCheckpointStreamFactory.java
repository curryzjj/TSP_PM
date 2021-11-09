import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class TestCheckpointStreamFactory implements CheckpointStreamFactory {
    private final Supplier<CheckpointStateOutputStream> supplier;

    public TestCheckpointStreamFactory(Supplier<CheckpointStateOutputStream> supplier) {
        this.supplier = checkNotNull(supplier);
    }

    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream(
            CheckpointedStateScope scope) {
        return supplier.get();
    }
}
