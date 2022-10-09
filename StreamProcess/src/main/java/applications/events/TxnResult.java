package applications.events;

public abstract class TxnResult {
    public long bid;
    public TxnResult(long bid){
        this.bid = bid;
    }
}
