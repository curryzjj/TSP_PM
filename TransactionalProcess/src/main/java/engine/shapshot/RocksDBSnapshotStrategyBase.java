package engine.shapshot;

import engine.shapshot.ShapshotResources.SnapshotResources;
import engine.storage.ImplStorageManager.RocksDBManager;
import engine.table.keyGroup.KeyGroupRange;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.LocalRecoveryConfig;
import utils.ResourceGuard;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;

public abstract class RocksDBSnapshotStrategyBase<R extends SnapshotResources> implements SnapshotStrategy<R>,CheckpointListener {
    private static final Logger LOG= LoggerFactory.getLogger(RocksDBSnapshotStrategyBase.class);
    @Nonnull private final String description;
    /**RocksDB instance from the backend.*/
    @Nonnull protected RocksDB db;
    /** Resource guard for the RocksDB instance. */
    @Nonnull protected final ResourceGuard rocksDBResourceGuard;
    /** Key/Value state meta info from the backend. */
    @Nonnull protected final LinkedHashMap<String, RocksDBManager.RocksDBKvStateInfo> kvStateInformation;
    @Nonnull
    public KeyGroupRange keyGroupRange;

    protected RocksDBSnapshotStrategyBase(@Nonnull String description,
                                          @Nonnull RocksDB db,
                                          @Nonnull ResourceGuard rocksDBResourceGuard,
                                          @Nonnull LinkedHashMap<String, RocksDBManager.RocksDBKvStateInfo> kvStateInformation,
                                          @Nonnull KeyGroupRange keyGroupRange) {
        this.description = description;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        this.kvStateInformation = kvStateInformation;
        this.db=db;
        this.keyGroupRange = keyGroupRange;
    }
    public String getDescription(){
        return description;
    }
}
