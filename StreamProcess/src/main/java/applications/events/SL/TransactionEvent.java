package applications.events.SL;

import applications.events.TxnEvent;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecordRef;

import java.util.List;

import static UserApplications.constants.StreamLedgerConstants.Constant.MIN_BALANCE;

public class TransactionEvent extends TxnEvent {
    //embeded state.
    public volatile SchemaRecordRef src_account_value = new SchemaRecordRef();
    public volatile SchemaRecordRef dst_account_value = new SchemaRecordRef();
    public volatile SchemaRecordRef src_asset_value = new SchemaRecordRef();
    public volatile SchemaRecordRef dst_asset_value = new SchemaRecordRef();


    public volatile TableRecordRef src_account_values = new TableRecordRef();
    public volatile TableRecordRef dst_account_values = new TableRecordRef();
    public volatile TableRecordRef src_asset_values = new TableRecordRef();
    public volatile TableRecordRef dst_asset_values = new TableRecordRef();

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
            long minAccountBalance) {
        super(bid, partition_id, bid_array, number_of_partitions);

        this.sourceAccountId = sourceAccountId;
        this.targetAccountId = targetAccountId;
        this.sourceBookEntryId = sourceBookEntryId;
        this.targetBookEntryId = targetBookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.minAccountBalance = minAccountBalance;
        this.timestamp=System.nanoTime();
    }

    public TransactionEvent(int bid, int partition_id, String bid_array, int num_of_partition,
                            String sourceAccountId,
                            String sourceBookEntryId,
                            String targetAccountId,
                            String targetBookEntryId,
                            long accountTransfer,
                            long bookEntryTransfer,long timestamp) {

        super(bid, partition_id, bid_array, num_of_partition);
        this.sourceAccountId = sourceAccountId;
        this.targetAccountId = targetAccountId;
        this.sourceBookEntryId = sourceBookEntryId;
        this.targetBookEntryId = targetBookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.minAccountBalance = MIN_BALANCE;
        this.timestamp=timestamp;
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


    public List<DataBox> getUpdatedSourceBalance() {
        return null;
    }

    public List<DataBox> getUpdatedTargetBalance() {
        return null;
    }

    public List<DataBox> getUpdatedSourceAsset_value() {
        return null;
    }

    public List<DataBox> getUpdatedTargetAsset_value() {
        return null;
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
}
