package engine.shapshot;


import System.FileSystem.Path;
import engine.table.keyGroup.KeyGroupRangeOffsets;

public class SnapshotResult {
    /** An singleton instance to represent an empty snapshot result. */
    private static SnapshotResult EMPTY = new SnapshotResult();
    public SnapshotResult() {
    }
    public static <T> SnapshotResult empty() {
        return (SnapshotResult) EMPTY;
    }
    private Path snapshotPath;
    private KeyGroupRangeOffsets keyGroupRangeOffsets;
    public SnapshotResult(Path snapshotPath, KeyGroupRangeOffsets keyGroupRangeOffsets){
        this.snapshotPath = snapshotPath;
        this.keyGroupRangeOffsets = keyGroupRangeOffsets;
    }

}
