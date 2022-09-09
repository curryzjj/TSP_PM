package applications.events.lr;

import applications.events.TxnResult;

public class TollProcessingResult extends TxnResult {
    //if abort toll = -1
    public double toll;
    public TollProcessingResult(long bid, long timeStamp, double toll) {
        super(bid, timeStamp);
        this.toll = toll;
    }
}
