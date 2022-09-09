package applications.events.ob;

import applications.events.TxnResult;

public class OnlineBidingResult extends TxnResult {
    public boolean result;
    public OnlineBidingResult(long bid, long timeStamp, boolean result) {
        super(bid, timeStamp);
        this.result = result;
    }
}
