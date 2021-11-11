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
}
