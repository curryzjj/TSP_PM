package applications.events.SL;

import applications.events.TxnEvent;
import engine.table.tableRecords.SchemaRecordRef;

import java.util.Arrays;

import static UserApplications.constants.StreamLedgerConstants.Constant.MAX_BALANCE;
import static UserApplications.constants.StreamLedgerConstants.Constant.MIN_BALANCE;

public class TransactionEvent extends TxnEvent {
    //embeded state.
    public SchemaRecordRef src_account_value = new SchemaRecordRef();
    public SchemaRecordRef src_asset_value = new SchemaRecordRef();

    public TransactionResult transaction_result;


    private String sourceAccountId;
    private String targetAccountId;
    private String sourceBookEntryId;
    private String targetBookEntryId;
    private long accountTransfer;
    private long bookEntryTransfer;
    private long minAccountBalance;

    /**
     * Creates a new TransactionEvent for the given accounts and book entries.
     */
    public TransactionEvent(
            long bid, int partition_id, long[] bid_array, int number_of_partitions,
            String sourceAccountId,
            String sourceBookEntryId,
            String targetAccountId,
            String targetBookEntryId,
            long accountTransfer,
            long bookEntryTransfer,
            long minAccountBalance,
            boolean isAbort) {
        super(bid, partition_id, bid_array, number_of_partitions,isAbort);
        this.sourceAccountId = sourceAccountId;
        this.targetAccountId = targetAccountId;
        this.sourceBookEntryId = sourceBookEntryId;
        this.targetBookEntryId = targetBookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.minAccountBalance = minAccountBalance;
        this.timestamp = System.nanoTime();
    }

    public TransactionEvent(int bid, int partition_id, String bid_array, int num_of_partition,
                            String sourceAccountId,
                            String sourceBookEntryId,
                            String targetAccountId,
                            String targetBookEntryId,
                            long accountTransfer,
                            long bookEntryTransfer,
                            long timestamp,
                            boolean isAbort) {

        super(bid, partition_id, bid_array, num_of_partition,isAbort);
        this.sourceAccountId = sourceAccountId;
        this.targetAccountId = targetAccountId;
        this.sourceBookEntryId = sourceBookEntryId;
        this.targetBookEntryId = targetBookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        if (isAbort) {
            this.minAccountBalance = MAX_BALANCE;
        } else {
            this.minAccountBalance = MIN_BALANCE;
        }
        this.timestamp = timestamp;
    }

    public String getSourceAccountId() {
        return sourceAccountId;
    }

    public String getTargetAccountId() {
        return targetAccountId;
    }

    public String getSourceBookEntryId() {
        return sourceBookEntryId;
    }

    public String getTargetBookEntryId() {
        return targetBookEntryId;
    }

    public long getAccountTransfer() {
        return accountTransfer;
    }

    public long getBookEntryTransfer() {
        return bookEntryTransfer;
    }


    public long getMinAccountBalance() {
        return minAccountBalance;
    }

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String split_exp = ";";
        sb.append(bid).append(split_exp);//0-bid
        sb.append(pid).append(split_exp);//1-pid
        sb.append(Arrays.toString(bid_array)).append(split_exp);//2-bid_array
        sb.append(num_p()).append(split_exp);//3 number of p
        sb.append("TransactionEvent").append(split_exp);//4 input_event type
        sb.append(getSourceAccountId()).append(split_exp);//5 S-accountId
        sb.append(getSourceBookEntryId()).append(split_exp);//6 S-bookId
        sb.append(getTargetAccountId()).append(split_exp);//7 T-accountId
        sb.append(getTargetBookEntryId()).append(split_exp);//8 T-bookId
        sb.append(getAccountTransfer()).append(split_exp);//9 accountTransfer
        sb.append(getBookEntryTransfer()).append(split_exp);//10 bookTransfer
        sb.append(timestamp).append(split_exp);//11-timestamp
        sb.append(isAbort);//12-isAbort
        return sb.toString();
    }

    @Override
    public TransactionEvent cloneEvent() {
        return new TransactionEvent((int)bid,pid, Arrays.toString(bid_array),number_of_partitions,sourceAccountId,sourceBookEntryId,targetAccountId,targetBookEntryId,accountTransfer,bookEntryTransfer,timestamp,isAbort);
    }
}
