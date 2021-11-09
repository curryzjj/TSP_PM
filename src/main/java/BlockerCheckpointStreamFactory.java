import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class BlockerCheckpointStreamFactory implements CheckpointStreamFactory {
    protected final int maxSize;
    protected volatile int afterNumberInvocations;
    protected volatile OneShotLatch blocker;
    protected volatile OneShotLatch waiter;

    protected final Set<BlockingCheckpointOutputStream> allCreatedStreams;

    public Set<BlockingCheckpointOutputStream> getAllCreatedStreams() {
        return allCreatedStreams;
    }

    public BlockerCheckpointStreamFactory(int maxSize) {
        this.maxSize = maxSize;
        this.allCreatedStreams = new HashSet<>();
    }

    public void setAfterNumberInvocations(int afterNumberInvocations) {
        this.afterNumberInvocations = afterNumberInvocations;
    }

    public void setBlockerLatch(OneShotLatch latch) {
        this.blocker = latch;
    }

    public void setWaiterLatch(OneShotLatch latch) {
        this.waiter = latch;
    }

    public OneShotLatch getBlockerLatch() {
        return blocker;
    }

    public OneShotLatch getWaiterLatch() {
        return waiter;
    }

    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream(
            CheckpointedStateScope scope) throws IOException {

        BlockingCheckpointOutputStream blockingStream =
                new BlockingCheckpointOutputStream(
                        new MemCheckpointStreamFactory.MemoryCheckpointOutputStream(maxSize),
                        waiter,
                        blocker,
                        afterNumberInvocations);

        allCreatedStreams.add(blockingStream);

        return blockingStream;
    }
}
