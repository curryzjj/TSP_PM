package applications.events.lr;

import applications.events.TxnResult;

public class TollProcessingResult extends TxnResult {
    // toll = -1; if abort
    double toll;
    public TollProcessingResult(long bid, long timeStamp, double toll) {
        super(bid, timeStamp);
        this.toll = toll;
    }
}
