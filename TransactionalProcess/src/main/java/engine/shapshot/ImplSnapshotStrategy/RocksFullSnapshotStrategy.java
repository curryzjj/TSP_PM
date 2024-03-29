package engine.shapshot.ImplSnapshotStrategy;

import engine.shapshot.CheckpointOptions;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.shapshot.FullSnapshotAsyncWrite;
import engine.shapshot.RocksDBSnapshotStrategyBase;
import engine.shapshot.ShapshotResources.FullSnapshotResources;
import engine.shapshot.ShapshotResources.ImplShapshotResources.RocksDBFullSnapshotResources;
import engine.shapshot.SnapshotResult;
import engine.storage.ImplStorageManager.RocksDBManager;
import engine.table.keyGroup.KeyGroupRange;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ResourceGuard;
import utils.SupplierWithException;
import utils.TransactionalProcessConstants;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

public class RocksFullSnapshotStrategy extends RocksDBSnapshotStrategyBase<FullSnapshotResources> {
    private static final Logger LOG= LoggerFactory.getLogger(RocksFullSnapshotStrategy.class);
    private static final String DESCRIPTION="Asynchronous full RocksDB snapshot";
    public RocksFullSnapshotStrategy(
                                     @NotNull RocksDB db,
                                     @NotNull ResourceGuard rocksDBResourceGuard,
                                     @Nonnull LinkedHashMap<String, RocksDBManager.RocksDBKvStateInfo> kvStateInfomation,
                                     @Nonnull KeyGroupRange keyGroupRange) {
        super(DESCRIPTION, db, rocksDBResourceGuard,kvStateInfomation, keyGroupRange);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }


    @Override
    public FullSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        return RocksDBFullSnapshotResources.create(kvStateInformation,db,rocksDBResourceGuard,keyGroupRange);
    }

    @Override
    public FullSnapshotResources syncPrepareResourcesByPartitionId(long checkpointId, int partitionId) throws Exception {
        return null;
    }

    @Override
    public List<FullSnapshotResources> syncPrepareResources(long checkpointId, int partitionNum) throws IOException {
        return null;
    }

    @Override
    public SnapshotResultSupplier asyncSnapshot(FullSnapshotResources snapshotResources,
                                                long checkpointId,
                                                long timestamp,
                                                @NotNull CheckpointStreamFactory streamFactory,
                                                @NotNull CheckpointOptions checkpointOptions) throws IOException {
        if (snapshotResources.getMetaInfoSnapshots().isEmpty()){
            if(LOG.isDebugEnabled()){
                LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null",timestamp);
            }
            return registy-> SnapshotResult.empty();
        }
        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                createCheckpointStreamSupplier(
                        checkpointId, streamFactory, checkpointOptions);
        return new FullSnapshotAsyncWrite(CheckpointStreamWithResultProvider.createSimpleStream(streamFactory),
                snapshotResources,
                TransactionalProcessConstants.CheckpointType.RocksDBFullSnapshot,
                timestamp,
                checkpointId);
    }

    @Override
    public SnapshotResultSupplier parallelSnapshot(List<FullSnapshotResources> resources, long checkpointId, long timestamp, @NotNull CheckpointStreamFactory streamFactory, @NotNull CheckpointOptions checkpointOptions) throws IOException {
        return null;
    }

    private SupplierWithException<CheckpointStreamWithResultProvider,Exception>
    createCheckpointStreamSupplier(long checkpointId,
                                   CheckpointStreamFactory primaryStreamFactory,
                                   CheckpointOptions checkpointOptions) throws IOException {
       return ()->CheckpointStreamWithResultProvider.createSimpleStream(primaryStreamFactory);
    }
}
