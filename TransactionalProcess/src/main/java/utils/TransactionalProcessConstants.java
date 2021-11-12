package utils;

public class TransactionalProcessConstants {
    public static String content_type;//init in the appRunner
    /**
     * An enum with the current supported types.
     */
    public enum DataBoxTypes {
        BOOL, INT, LONG, TimestampType, FLOAT, STRING, OTHERS
    }
    public enum CheckpointType{
        FullSnapshot
    }
    /** Determines how the write-part if snapshot should be executed. */
    public enum SnapshotExecutionType{
        SYNCHRONOUS,
        ASYNCHRONOUS
    }
    /** Enum that defines the different types of state that live in TSP backends. */
    public enum BackendStateType {
        KEY_VALUE,
    }
}
