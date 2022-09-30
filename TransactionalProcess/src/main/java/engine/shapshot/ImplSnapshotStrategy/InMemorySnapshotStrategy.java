package engine.shapshot.ImplSnapshotStrategy;

import System.tools.StringHelper;
import engine.shapshot.*;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.shapshot.ShapshotResources.FullSnapshotResources;
import engine.shapshot.ShapshotResources.ImplShapshotResources.InMemoryFullSnapshotResources;
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
import java.util.*;

public class InMemorySnapshotStrategy extends InMemorySnapshotStrategyBase<FullSnapshotResources> {
    private static final Logger LOG= LoggerFactory.getLogger(InMemorySnapshotStrategy.class);
    private static final String DESCRIPTION="Asynchronous full In-Memory snapshot";
    public InMemorySnapshotStrategy(@Nonnull Map<String, BaseTable> tables,
                                       @NotNull ResourceGuard ResourceGuard,
                                       @Nonnull LinkedHashMap<String, StorageManager.InMemoryKvStateInfo> kvStateInformation,
                                       @Nonnull KeyGroupRange keyGroupRange) {
        super(DESCRIPTION, tables, ResourceGuard, kvStateInformation, keyGroupRange);
    }

    @Override
    public FullSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        return InMemoryFullSnapshotResources.create(kvStateInformation,tables, resourceGuard,keyGroupRange, checkpointId);
    }

    @Override
    public List<FullSnapshotResources> syncPrepareResources(long checkpointId, int partitionNum) throws IOException {
        List<FullSnapshotResources> resources=new ArrayList<>();
        for(int i = 0;i < partitionNum; i++){
            Map<String,BaseTable> tables = new HashMap<>();
            LinkedHashMap<String,StorageManager.InMemoryKvStateInfo> kvStateInformation = new LinkedHashMap<>();
            for (String tableName : this.tables.keySet()){
                if(StringHelper.isDigitStr(tableName) == i){
                    tables.put(tableName, this.tables.get(tableName));
                    kvStateInformation.put(tableName, this.kvStateInformation.get(tableName));
                }
            }
            resources.add(InMemoryFullSnapshotResources.create(kvStateInformation, tables, resourceGuard, keyGroupRange, checkpointId));
        }
        return resources;
    }

    @Override
    public FullSnapshotResources syncPrepareResourcesByPartitionId(long checkpointId, int partitionId) throws Exception {
        Map<String,BaseTable> tables = new HashMap<>();
        LinkedHashMap<String,StorageManager.InMemoryKvStateInfo> kvStateInformation = new LinkedHashMap<>();
        for (String tableName : this.tables.keySet()){
            if(StringHelper.isDigitStr(tableName) == partitionId){
                tables.put(tableName, this.tables.get(tableName));
                kvStateInformation.put(tableName, this.kvStateInformation.get(tableName));
            }
        }
        return InMemoryFullSnapshotResources.create(kvStateInformation, tables, resourceGuard, keyGroupRange, checkpointId);
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
        return new FullSnapshotAsyncWrite(CheckpointStreamWithResultProvider.createSimpleStream(streamFactory),
                    snapshotResources,
                    TransactionalProcessConstants.CheckpointType.InMemoryFullSnapshot,
                    timestamp,
                    checkpointId);
    }

    @Override
    public SnapshotResultSupplier parallelSnapshot(List<FullSnapshotResources> snapshotResources, long checkpointId, long timestamp, @NotNull CheckpointStreamFactory streamFactory, @NotNull CheckpointOptions checkpointOptions) throws IOException {
           return new ParallelFullSnapshotWrite(CheckpointStreamWithResultProvider.createMultipleStream(streamFactory,checkpointOptions.partitionNum),
                snapshotResources,
                timestamp,
                checkpointId,
                TransactionalProcessConstants.CheckpointType.InMemoryFullSnapshot,checkpointOptions);
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
