package engine.shapshot;


public class SnapshotResult {
    /** An singleton instance to represent an empty snapshot result. */
    private static SnapshotResult EMPTY = new SnapshotResult();
    public static <T> SnapshotResult empty() {
        return (SnapshotResult) EMPTY;
    }
}
