package engine.transaction;

import engine.Meta.MetaTypes;
import engine.table.tableRecords.SchemaRecordRef;
import engine.transaction.common.Operation;
import engine.transaction.common.OperationChain;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TxnContext {
    public final int thread_Id;
    private final int fid;
    private final long bid;
    public int pid;
    public AtomicBoolean isAbort = new AtomicBoolean(false);
    private ConcurrentHashMap<Operation, OperationChain> brotherOperations = new ConcurrentHashMap<>();

    public TxnContext(int thread_Id, int fid, long bid) {
        this.thread_Id = thread_Id;
        this.fid = fid;
        this.bid = bid;
    }
    public long getBID() {
        return bid;
    }

    //Handle Operation Abort in the same Transaction
    public void addBrotherOperations(Operation operation, OperationChain operationChain) {
        this.brotherOperations.put(operation, operationChain);
    }
    public void checkTransactionAbort(Operation operation, OperationChain operationChain) {
        operationChain.needAbortHandling.compareAndSet(false, true);
        operationChain.failedOperations.add(operation);
        isAbort.compareAndSet(false,true);
        for (Map.Entry<Operation, OperationChain> ops : brotherOperations.entrySet()) {
            if (!ops.getKey().accessType.equals(MetaTypes.AccessType.READ_ONLY)) {
                ops.getValue().needAbortHandling.compareAndSet(false, true);
                if (!ops.getValue().failedOperations.contains(operation)) {
                    ops.getValue().failedOperations.add(operation);
                }
            }
        }
    }
}
