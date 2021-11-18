package engine.shapshot.ShapshotResources;

import engine.shapshot.StateMetaInfoSnapshot;
import engine.table.keyGroup.KeyGroupRange;
import utils.StateIterator.RocksDBStateIterator;
import utils.StateIterator.kvStateIterator;

import java.io.IOException;
import java.util.List;

public interface FullSnapshotResources extends SnapshotResources{
    /**
     * Returns the list of{@link StateMetaInfoSnapshot meta info snapshots} for this state
     * @return
     */
    List<StateMetaInfoSnapshot> getMetaInfoSnapshots();

    /**
     * Returns a {@link RocksDBStateIterator} for iterating over all K-V states for this snapshot resources.
     * @return
     * @throws IOException
     */
    kvStateIterator createKVStateIterator() throws IOException;
    /** Returns the {@link KeyGroupRange} of this snapshot. */
    KeyGroupRange getKeyGroupRange();
}
