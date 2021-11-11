package engine.checkpoint;

import engine.checkpoint.CheckpointStream.CheckpointStreamFactory;
import engine.checkpoint.ShapshotResources.SnapshotResources;
import utils.CloseableRegistry.CloseableRegistry;

import javax.annotation.Nonnull;
import java.io.IOException;

public interface SnapshotStrategy<SR extends SnapshotResources>{
    /**
     * Performs the synchronous part of the snapshot. It returns resources which can be later
     * on used in the asynchronous
     * @param checkpointId the ID of the checkpoint
     * @return Resources needed to finish the snapshot
     * @throws Exception
     */
    SR syncPrepareResources(long checkpointId) throws Exception;
    /**
     * Operation that writes a snapshot into a stream that is provided by the given {@link CheckpointStreamFactory}
     * and returns a{@link SnapshotResultSupplier} that gives a handle to the snapshot
     *
     * @param checkpointId The ID of the checkpoint.
     * @param timestamp The timestamp of the checkpoint.
     * @param streamFactory The factory that we can use for writing our state to streams.
     * @param checkpointOptions Options for how to perform the checkpoint
     * @return A supplier that will yield the {@link SnapshotResult}
     */
    SnapshotResultSupplier asyncSnapshot(
            SR snapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions
    ) throws IOException;

    /**
     * A supplier for a {@link SnapshotResult} with an access to a {@link CloseableRegistry} for
     * io tasks that need to be closed when cancelling the async part of the checkpoint.
     */
    interface SnapshotResultSupplier{
        /**
         * Performs the asynchronous part of a checkpoint and returns the snapshot results
         * @param snapshotCloseableRegistry A registry for io tasks to close on cancel
         * @return A snapshot result
         */
        SnapshotResult get(CloseableRegistry snapshotCloseableRegistry) throws Exception;
    }
}
