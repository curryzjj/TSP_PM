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
        return "TransactionEvent {"
                + "sourceAccountId=" + sourceAccountId
                + ", targetAccountId=" + targetAccountId
                + ", sourceBookEntryId=" + sourceBookEntryId
                + ", targetBookEntryId=" + targetBookEntryId
                + ", accountTransfer=" + accountTransfer
                + ", bookEntryTransfer=" + bookEntryTransfer
                + ", minAccountBalance=" + minAccountBalance
                + '}';
    }

    @Override
    public TransactionEvent cloneEvent() {
        return new TransactionEvent((int)bid,pid, Arrays.toString(bid_array),number_of_partitions,sourceAccountId,sourceBookEntryId,targetAccountId,targetBookEntryId,accountTransfer,bookEntryTransfer,timestamp,isAbort);
    }
}
