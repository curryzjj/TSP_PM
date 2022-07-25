package engine.transaction;

import engine.Meta.MetaTypes;
import engine.transaction.common.OperationChain;
import engine.transaction.common.Operation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TxnContext {
    public final int thread_Id;
    private final int fid;
    private final long bid;
    public int pid;
    private boolean is_read_only_;
    private ConcurrentHashMap<Operation, OperationChain> brotherOperations;

    public TxnContext(int thread_Id, int fid, long bid) {
        this.thread_Id = thread_Id;
        this.fid = fid;
        this.bid = bid;
    }

    public void addBrotherOperations(Operation operation, OperationChain operationChain) {
        this.brotherOperations.put(operation, operationChain);
    }

    public long getBID() {
        return bid;
    }

    public void checkTransactionAbort(Operation operation, OperationChain operationChain) {
        operationChain.needAbortHandling.compareAndSet(false, true);
        operationChain.failedOperations.add(operation);
        for (Map.Entry<Operation, OperationChain> ops : brotherOperations.entrySet()) {
            if (!ops.getKey().accessType.equals(MetaTypes.AccessType.READ_ONLY)) {
                ops.getValue().needAbortHandling.compareAndSet(false, true);
                if ( !ops.getValue().failedOperations.contains(operation)) {
                    ops.getValue().failedOperations.add(operation);
                }
            }
        }
    }
}
