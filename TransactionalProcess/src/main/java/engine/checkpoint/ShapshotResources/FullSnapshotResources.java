package engine.checkpoint.ShapshotResources;

import engine.checkpoint.StateMetaInfoSnapshot;
import utils.StateIterator.KeyValueStateIterator;

import java.io.IOException;
import java.util.List;

public interface FullSnapshotResources extends SnapshotResources{
    /**
     * Returns the list of{@link StateMetaInfoSnapshot meta info snapshots} for this state
     * @return
     */
    List<StateMetaInfoSnapshot> getMetaInfoSnapshots();

    /**
     * Returns a {@link KeyValueStateIterator} for iterating over all K-V states for this snapshot resources.
     * @return
     * @throws IOException
     */
    KeyValueStateIterator createKVStateIterator() throws IOException;
}
