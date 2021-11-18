package engine.shapshot;

import engine.shapshot.ShapshotResources.SnapshotResources;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.BaseTable;
import engine.table.keyGroup.KeyGroupRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ResourceGuard;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class InMemorySnapshotStrategyBase<R extends SnapshotResources> implements SnapshotStrategy<R>,CheckpointListener {
    private static final Logger LOG= LoggerFactory.getLogger(InMemorySnapshotStrategyBase.class);
    @Nonnull
    private final String description;
    /**RocksDB instance from the backend.*/
    @Nonnull protected Map<String, BaseTable> tables;
    /** Resource guard for the RocksDB instance. */
    @Nonnull protected final ResourceGuard resourceGuard;
    /** Key/Value state meta info from the backend. */
    @Nonnull protected final LinkedHashMap<String, StorageManager.InMemoryKvStateInfo> kvStateInformation;
    @Nonnull
    public KeyGroupRange keyGroupRange;

    protected InMemorySnapshotStrategyBase(@Nonnull String description,
                                          @Nonnull Map<String,BaseTable> tables,
                                          @Nonnull ResourceGuard ResourceGuard,
                                          @Nonnull LinkedHashMap<String, StorageManager.InMemoryKvStateInfo> kvStateInformation,
                                          @Nonnull KeyGroupRange keyGroupRange) {
        this.description = description;
        this.resourceGuard = ResourceGuard;
        this.kvStateInformation = kvStateInformation;
        this.tables=tables;
        this.keyGroupRange = keyGroupRange;
    }
    public String getDescription(){
        return description;
    }
}
