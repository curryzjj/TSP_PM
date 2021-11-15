package engine.shapshot;

import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.ShapshotResources.SnapshotResources;
import utils.CloseableRegistry.CloseableRegistry;

import javax.annotation.Nonnull;
import java.io.IOException;

public interface SnapshotStrategy<SR extends SnapshotResources>{
    /**
     * Performs the synchronous part of the snapshot. It returns resources which can be later
     * on used in the asynchronous
     * @param checkpointId the ID of the shapshot
     * @return Resources needed to finish the snapshot
     * @throws Exception
     */
    SR syncPrepareResources(long checkpointId) throws Exception;
    /**
     * Operation that writes a snapshot into a stream that is provided by the given {@link CheckpointStreamFactory}
     * and returns a{@link SnapshotResultSupplier} that gives a handle to the snapshot
     *
     * @param checkpointId The ID of the shapshot.
     * @param timestamp The timestamp of the shapshot.
     * @param streamFactory The factory that we can use for writing our state to streams.
     * @param checkpointOptions Options for how to perform the shapshot
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
     * io tasks that need to be closed when cancelling the async part of the shapshot.
     */
    interface SnapshotResultSupplier{
        /**
         * Performs the asynchronous part of a shapshot and returns the snapshot results
         * @param snapshotCloseableRegistry A registry for io tasks to close on cancel
         * @return A snapshot result
         */
        SnapshotResult get(CloseableRegistry snapshotCloseableRegistry) throws Exception;
    }
}
