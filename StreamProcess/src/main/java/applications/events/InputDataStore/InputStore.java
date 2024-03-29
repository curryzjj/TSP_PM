package applications.events.InputDataStore;

import System.tools.SortHelper;
import applications.events.TxnEvent;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Persist Input event
 */
public abstract class InputStore implements Serializable {
    private static final long serialVersionUID = -5256417817969129467L;
    protected String inputFile;
    //<checkpointOffset, Path>
    protected HashMap<Long, String> inputStorePaths = new HashMap<>();
    protected long currentOffset;
    protected final String split_exp = ";";
    public abstract void storeInput(List<TxnEvent> inputs) throws IOException;
    public abstract void close();
    public void initialize(String path) {
        this.currentOffset = 0L;
        this.inputFile = path;
        File file = new File(inputFile);
        if (!file.exists()) {
            file.mkdirs();
        }
        this.inputStorePaths.put(currentOffset, UUID.randomUUID().toString());
    }
    public void switchInputStorePath(long currentOffset) {
        this.currentOffset = currentOffset;
        this.inputStorePaths.putIfAbsent(currentOffset, UUID.randomUUID().toString());
        File file = new File(inputFile.concat(this.inputStorePaths.get(currentOffset)));
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public List<Long> getStoredSnapshotOffsets(long lastSnapshotOffset) {
        Set<Long> keySets  = new HashSet<>();
        for (long offset : this.inputStorePaths.keySet()) {
            if (offset >= lastSnapshotOffset) {
                keySets.add(offset);
            }
        }
        return SortHelper.sortKey(keySets);
    }
    public int checkpointNum(long lastSnapshotOffset) {
        int number = 0;
        for (long offset : this.inputStorePaths.keySet()) {
            if (offset >= lastSnapshotOffset) {
                number ++;
            }
        }
        return number;
    }
    public String getInputStorePath(long offset) {
        return this.inputStorePaths.get(offset);
    }
}
