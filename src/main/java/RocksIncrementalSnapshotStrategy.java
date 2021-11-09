import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateUploader;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.ResourceGuard;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class RocksIncrementalSnapshotStrategy<K> extends RocksDBSnapshotStrategyBase<K,RocksIncrementalSnapshotStrategy.IncrementalRocksDBSnapshotResources> {
    private static final Logger LOG= LoggerFactory.getLogger(RocksIncrementalSnapshotStrategy.class);
    private static final String DESCRIPTION="Asynchronous increment RocksDB snapshot";
    //Base path of the RocksDB instance
    @Nonnull private final File instanceBasePath;
    //The state handle ids if all sst files materialized in snapshot for previous checkpoints.
    @Nonnull private final UUID backendUID;
    //Stores the materialized sstable files from all snapshots that build the incremental history.
    @Nonnull private final SortedMap<Long,Set<StateHandleID>> materializedSstFiles;

    /** The identifier of the last completed checkpoint. */
    private long lastCompletedCheckpointId;
    /** The help class used to upload state files. */
    private final RocksDBStateUploader stateUploader;
    /** The local directory name of the current snapshot strategy. */
    private final String localDirectoryName;
    public RocksIncrementalSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            @Nonnull File instanceBasePath,
            @Nonnull UUID backendUID,
            @Nonnull SortedMap<Long, Set<StateHandleID>> materializedSstFiles,
            long lastCompletedCheckpointId,
            int numberOfTransferingThreads) {
        super(
                DESCRIPTION,
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                localRecoveryConfig);

        this.instanceBasePath = instanceBasePath;
        this.backendUID = backendUID;
        this.materializedSstFiles = materializedSstFiles;
        this.lastCompletedCheckpointId = lastCompletedCheckpointId;
        this.stateUploader = new RocksDBStateUploader(numberOfTransferingThreads);
        this.localDirectoryName = backendUID.toString().replaceAll("[\\-]", "");
    }

    @Override
    public void close() {

    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {

    }

    @Override
    public IncrementalRocksDBSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        final SnapshotDirectory snapshotDirectory=prepareLocalSnapshotDirectory(checkpointId);
        LOG.trace("Local RocksDB checkpoint goes to backup path {}.", snapshotDirectory);
        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots=new ArrayList<>(kvStateInformation.size());
        final Set<StateHandleID> baseSstFiles=snapshotMetaData(checkpointId,stateMetaInfoSnapshots);

        return null;
    }
    private Set<StateHandleID> snapshotMetaData(long checkpointId,List<StateMetaInfoSnapshot> stateMetaInfoSnapshots){
        final long lastCompletedCheckpoint;
        final Set<StateHandleID> baseSstFiles;
        //use the last completed checkpoint as the comparison base
        synchronized (materializedSstFiles) {
            lastCompletedCheckpoint = lastCompletedCheckpointId;
            baseSstFiles = materializedSstFiles.get(lastCompletedCheckpoint);
        }
        LOG.trace(
                "Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} "
                        + "assuming the following (shared) files as base: {}.",
                checkpointId,
                lastCompletedCheckpoint,
                baseSstFiles);
        // snapshot meta data to save
        for (Map.Entry<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }
        return baseSstFiles;
    }
    private void takeDBNativeCheckpoint(SnapshotDirectory outputDirectory) throws Exception{
        //create hard links of living files in the output path
    }
    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(IncrementalRocksDBSnapshotResources incrementalRocksDBSnapshotResources, long l, long l1, @NotNull CheckpointStreamFactory checkpointStreamFactory, @NotNull CheckpointOptions checkpointOptions) {
        return null;
    }
    private SnapshotDirectory prepareLocalSnapshotDirectory(long checkpointId) throws IOException{
        if(localRecoveryConfig.isLocalRecoveryEnabled()){
            //create a "permanent" snapshot directory for local recovery
            File directory=new File(System.getProperty("user.home").concat("/hair-loss/app/Checkpoint/"));
            if (!directory.exists() && !directory.mkdirs()) {
                throw new IOException(
                        "Local state base directory for checkpoint "
                                + checkpointId
                                + " does not exist and could not be created: "
                                + directory);
            }
            File rdbSnapshotDir =new File(directory,localDirectoryName);
            if (rdbSnapshotDir.exists()) {
                FileUtils.deleteDirectory(rdbSnapshotDir);
            }
            Path path = rdbSnapshotDir.toPath();
            try {
                return SnapshotDirectory.permanent(path);
            } catch (IOException ex) {
                try {
                    FileUtils.deleteDirectory(directory);
                } catch (IOException delEx) {
                    ex = ExceptionUtils.firstOrSuppressed(delEx, ex);
                }
                throw ex;
            }
        }
        return null;
    }

    static class IncrementalRocksDBSnapshotResources implements SnapshotResources {
        @Nonnull
        private final SnapshotDirectory snapshotDirectory;
        @Nonnull private final Set<StateHandleID> baseSstFiles;
        @Nonnull private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        public IncrementalRocksDBSnapshotResources(
                SnapshotDirectory snapshotDirectory,
                Set<StateHandleID> baseSstFiles,
                List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
            this.snapshotDirectory = snapshotDirectory;
            this.baseSstFiles = baseSstFiles;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        }

        @Override
        public void release() {
            try {
                if (snapshotDirectory.exists()) {
                    LOG.trace(
                            "Running cleanup for local RocksDB backup directory {}.",
                            snapshotDirectory);
                    boolean cleanupOk = snapshotDirectory.cleanup();

                    if (!cleanupOk) {
                        LOG.debug("Could not properly cleanup local RocksDB backup directory.");
                    }
                }
            } catch (IOException e) {
                LOG.warn("Could not properly cleanup local RocksDB backup directory.", e);
            }
        }
    }
}
