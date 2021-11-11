package engine.checkpoint.ImplSnapshotStrategy;

import engine.checkpoint.CheckpointOptions;
import engine.checkpoint.CheckpointStream.CheckpointStreamFactory;
import engine.checkpoint.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.checkpoint.FullSnapshotAsyncWrite;
import engine.checkpoint.RocksDBSnapshotStrategyBase;
import engine.checkpoint.ShapshotResources.FullSnapshotResources;
import engine.checkpoint.ShapshotResources.ImplShapshotResources.RocksDBFullSnapshotResources;
import engine.checkpoint.SnapshotResult;
import engine.storage.ImplStorageManager.RocksDBManager;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.LocalRecoveryConfig;
import utils.ResourceGuard;
import utils.SupplierWithException;
import utils.TransactionalProcessConstants;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.LinkedHashMap;

public class RocksFullSnapshotStrategy extends RocksDBSnapshotStrategyBase<FullSnapshotResources> {
    private static final Logger LOG= LoggerFactory.getLogger(RocksFullSnapshotStrategy.class);
    private static final String DESCRIPTION="Asynchronous full RocksDB snapshot";
    protected RocksFullSnapshotStrategy(@NotNull String description,
                                        @NotNull RocksDB db,
                                        @NotNull ResourceGuard rocksDBResourceGuard,
                                        @Nonnull LinkedHashMap<String, RocksDBManager.RocksDBKvStateInfo> kvStateInfomation,
                                        @NotNull LocalRecoveryConfig localRecoveryConfig) {
        super(description, db, rocksDBResourceGuard,kvStateInfomation, localRecoveryConfig);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }


    @Override
    public FullSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        return RocksDBFullSnapshotResources.create(kvStateInformation,db,rocksDBResourceGuard);
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
        return new FullSnapshotAsyncWrite(checkpointStreamSupplier,
                snapshotResources,
                TransactionalProcessConstants.CheckpointType.FullSnapshot);
    }
    private SupplierWithException<CheckpointStreamWithResultProvider,Exception>
    createCheckpointStreamSupplier(long checkpointId,
                                   CheckpointStreamFactory primaryStreamFactory,
                                   CheckpointOptions checkpointOptions) throws IOException {
       return ()->CheckpointStreamWithResultProvider.createSimpleStream(primaryStreamFactory);
    }
}
