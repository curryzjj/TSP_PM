package applications.events.gs;

import applications.events.TxnResult;

public class MicroResult extends TxnResult {
    private boolean isAbort;
    private int sum;
    public MicroResult(long bid, long timestamp, boolean isAbort, int sum) {
        super(bid, timestamp);
        this.isAbort = isAbort;
        this.sum = sum;
    }
}
