package applications.events.ob;

import applications.events.TxnResult;

public class OnlineBidingResult extends TxnResult {
    public double result;
    public OnlineBidingResult(long bid, double result) {
        super(bid);
        this.result = result;
    }
}
