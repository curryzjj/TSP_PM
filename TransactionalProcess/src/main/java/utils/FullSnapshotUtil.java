package utils;
/**
 * Utility methods and constants around creating and restoring full snapshots using {@link
 * engine.shapshot.FullSnapshotAsyncWrite}.
 */
public class FullSnapshotUtil {
    public static final int FIRST_BIT_IN_BYTE_MASK = 0x80;

    public static final int END_OF_KEY_GROUP_MARK = 0xFFFF;

    public static void setMetaDataFollowsFlagInKey(byte[] key) {
        key[0] |= FIRST_BIT_IN_BYTE_MASK;
    }

    public static void clearMetaDataFollowsFlag(byte[] key) {
        key[0] &= (~FIRST_BIT_IN_BYTE_MASK);
    }

    public static boolean hasMetaDataFollowsFlag(byte[] key) {
        return 0 != (key[0] & FIRST_BIT_IN_BYTE_MASK);
    }

    private FullSnapshotUtil() {
        throw new AssertionError();
    }
}
