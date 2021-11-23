package utils;
/**
 * Utility methods and constants around creating and restoring full snapshots using {@link
 * engine.shapshot.FullSnapshotAsyncWrite}.
 */
public class FullSnapshotUtil {
    public static final int END_OF_KEY_GROUP_MARK = 0xFFFF;
    private FullSnapshotUtil() {
        throw new AssertionError();
    }
}
