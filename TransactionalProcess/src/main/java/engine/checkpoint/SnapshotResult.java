package engine.checkpoint;

import org.apache.flink.runtime.state.StateObject;

public class SnapshotResult {
    /** An singleton instance to represent an empty snapshot result. */
    private static SnapshotResult EMPTY = new SnapshotResult();
    public static <T extends StateObject> SnapshotResult empty() {
        return (SnapshotResult) EMPTY;
    }
}
