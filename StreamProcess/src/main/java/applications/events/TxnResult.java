package applications.events;

public abstract class TxnResult {
    public long bid;
    public long timeStamp;
    public TxnResult(long bid, long timeStamp){
        this.bid = bid;
        this.timeStamp = timeStamp;
    }
}
