package utils;

public class TransactionalProcessConstants {
    public static String content_type;//init in the appRunner
    /**
     * An enum with the current supported types.
     */
    public enum DataBoxTypes {
        BOOL, INT, LONG, TimestampType, DOUBLE,FLOAT, STRING, OTHERS
    }
    public enum CheckpointType{
        RocksDBFullSnapshot,InMemoryFullSnapshot
    }
    /** Determines how the write-part if snapshot should be executed. */
    public enum SnapshotExecutionType{
        SYNCHRONOUS,
        ASYNCHRONOUS
    }
    /** Determines how the write-part if commit-log should be executed. */
    public enum CommitLogExecutionType {
        SYNCHRONOUS,
        ASYNCHRONOUS
    }
    /** Enum that defines the different types of state that live in TSP backends. */
    public enum BackendStateType {
        KEY_VALUE,
    }
    public interface FaultTolerance{
        int END_OF_GLOBAL_LSN_MARK = 0;
    }
}