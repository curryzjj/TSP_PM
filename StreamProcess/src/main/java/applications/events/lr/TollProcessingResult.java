package applications.events.lr;

import applications.events.TxnResult;

public class TollProcessingResult extends TxnResult {
    //if abort toll = -1
    public double toll;
    public TollProcessingResult(long bid, double toll) {
        super(bid);
        this.toll = toll;
    }
}
