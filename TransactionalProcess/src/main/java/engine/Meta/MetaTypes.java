package engine.Meta;

public interface MetaTypes {

    enum LockType {
        NO_LOCK, READ_LOCK, WRITE_LOCK, CERTIFY_LOCK
    }

    enum AccessType {
        READ_ONLY, READS_ONLY, READ_WRITE, READ_WRITE_READ, READ_WRITE_COND, READ_WRITE_COND_READ, WRITE_ONLY, INSERT_ONLY, DELETE_ONLY
    }
    enum OperationStateType {
        READY, EXECUTED, ABORTED
    }
    enum DataBaseType {
        In_Memory,RocksDB
    }
}
