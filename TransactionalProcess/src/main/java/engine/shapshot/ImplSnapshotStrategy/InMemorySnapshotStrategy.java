package engine.shapshot.ImplSnapshotStrategy;

import engine.shapshot.CheckpointOptions;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.shapshot.FullSnapshotAsyncWrite;
import engine.shapshot.InMemorySnapshotStrategyBase;
import engine.shapshot.ShapshotResources.FullSnapshotResources;
import engine.shapshot.ShapshotResources.ImplShapshotResources.InMemoryFullSnapshotResources;
import engine.shapshot.SnapshotResult;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.BaseTable;
import engine.table.keyGroup.KeyGroupRange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ResourceGuard;
import utils.SupplierWithException;
import utils.TransactionalProcessConstants;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class InMemorySnapshotStrategy extends InMemorySnapshotStrategyBase<FullSnapshotResources> {
    private static final Logger LOG= LoggerFactory.getLogger(InMemorySnapshotStrategy.class);
    private static final String DESCRIPTION="Aynchronous full In-Memory snapshot";
    public InMemorySnapshotStrategy(@Nonnull Map<String, BaseTable> tables,
                                       @NotNull ResourceGuard ResourceGuard,
                                       @Nonnull LinkedHashMap<String, StorageManager.InMemoryKvStateInfo> kvStateInformation,
                                       @Nonnull KeyGroupRange keyGroupRange) {
        super(DESCRIPTION, tables, ResourceGuard, kvStateInformation, keyGroupRange);
    }

    @Override
    public FullSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        return InMemoryFullSnapshotResources.create(kvStateInformation,tables, resourceGuard,keyGroupRange);
    }

    @Override
    public SnapshotResultSupplier asyncSnapshot(FullSnapshotResources snapshotResources,
                                                long checkpointId,
                                                long timestamp,
                                                @NotNull CheckpointStreamFactory streamFactory,
                                                @NotNull CheckpointOptions checkpointOptions) throws IOException {
        if (snapshotResources.getMetaInfoSnapshots().isEmpty()){
            if(LOG.isDebugEnabled()){
                LOG.debug("In_memory snapshot performed on empty keyed state at {}. Returning null",timestamp);
            }
            return registy-> SnapshotResult.empty();
        }
        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                createCheckpointStreamSupplier(
                        checkpointId, streamFactory, checkpointOptions);
        return new FullSnapshotAsyncWrite(checkpointStreamSupplier,
                snapshotResources,
                TransactionalProcessConstants.CheckpointType.InMemoryFullSnapshot,
                timestamp,
                checkpointId);
    }
    private SupplierWithException<CheckpointStreamWithResultProvider,Exception>
    createCheckpointStreamSupplier(long checkpointId,
                                   CheckpointStreamFactory primaryStreamFactory,
                                   CheckpointOptions checkpointOptions) throws IOException {
        return ()->CheckpointStreamWithResultProvider.createSimpleStream(primaryStreamFactory);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

}
