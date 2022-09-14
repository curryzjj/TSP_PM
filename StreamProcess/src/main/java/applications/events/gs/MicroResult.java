package applications.events.gs;

import applications.events.TxnResult;

public class MicroResult extends TxnResult {
    public int sum;
    public MicroResult(long bid, int sum) {
        super(bid);
        this.sum = sum;
    }
}
