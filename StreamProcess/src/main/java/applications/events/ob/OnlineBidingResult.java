package applications.events.ob;

import applications.events.TxnResult;

public class OnlineBidingResult extends TxnResult {
    boolean result;
    public OnlineBidingResult(long bid, long timeStamp, boolean result) {
        super(bid, timeStamp);
        this.result = result;
    }
}
