package engine.shapshot;


import System.FileSystem.Path;
import engine.table.keyGroup.KeyGroupRangeOffsets;

import java.io.Serializable;

public class SnapshotResult implements Serializable {
    private static final long serialVersionUID = 8003076517462798444L;
    /** An singleton instance to represent an empty snapshot result. */
    private static SnapshotResult EMPTY = new SnapshotResult();
    public SnapshotResult() {
    }
    public static <T> SnapshotResult empty() {
        return (SnapshotResult) EMPTY;
    }
    private Path snapshotPath;
    private KeyGroupRangeOffsets keyGroupRangeOffsets;
    private long checkpointId;
    private long timestamp;
    public SnapshotResult(Path snapshotPath, KeyGroupRangeOffsets keyGroupRangeOffsets,long timestamp,long checkpointId){
        this.snapshotPath = snapshotPath;
        this.keyGroupRangeOffsets = keyGroupRangeOffsets;
        this.checkpointId=checkpointId;
        this.timestamp=timestamp;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Path getSnapshotPath() {
        return snapshotPath;
    }

    public KeyGroupRangeOffsets getKeyGroupRangeOffsets() {
        return keyGroupRangeOffsets;
    }
}
