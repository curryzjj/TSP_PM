package applications.events.SL;


import applications.events.TxnResult;

public class TransactionResult extends TxnResult {
    private boolean success;
    private long newSourceAccountBalance;
    private long newTargetAccountBalance;
    public TransactionResult(
            long bid,
            long timeStemp,
            boolean success,
            long newSourceAccountBalance,
            long newTargetAccountBalance) {
        super(bid, timeStemp);
        this.success = success;
        this.newSourceAccountBalance = newSourceAccountBalance;
        this.newTargetAccountBalance = newTargetAccountBalance;
    }
    public TransactionResult(long bid, long timeStamp, boolean success){
        super(bid, timeStamp);
        this.success = success;
    }
}
