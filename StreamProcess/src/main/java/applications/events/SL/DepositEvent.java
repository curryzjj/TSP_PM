package applications.events.SL;

import applications.events.TxnEvent;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.table.tableRecords.TableRecordRef;

import java.util.Arrays;
import java.util.List;

public class DepositEvent extends TxnEvent {
    //updated state...to be written.
    private String accountId; //32 bytes
    private String bookEntryId; //32 bytes
    private long accountTransfer; //64 bytes
    private long bookEntryTransfer;//64 bytes
    public TransactionResult transactionResult;
    /**
     * Creates a new DepositEvent.
     */
    public DepositEvent(
            long bid, int partition_id, long[] bid_array, int number_of_partitions,
            String accountId,
            String bookEntryId,
            long accountTransfer,
            long bookEntryTransfer,
            boolean isAbort) {
        super(bid, partition_id, bid_array, number_of_partitions,isAbort);
        this.accountId = accountId;
        this.bookEntryId = bookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.timestamp = System.nanoTime();
    }

    /**
     * Loading a DepositEvent.
     *
     * @param bid
     * @param pid
     * @param bid_array
     * @param num_of_partition
     * @param accountId
     * @param bookEntryId
     * @param accountTransfer
     * @param bookEntryTransfer
     */
    public DepositEvent(int bid, int pid, String bid_array, int num_of_partition,
                        String accountId,
                        String bookEntryId,
                        long accountTransfer,
                        long bookEntryTransfer,
                        long timestamp,
                        boolean isAbort) {
        super(bid, pid, bid_array, num_of_partition,isAbort);
        this.accountId = accountId;
        this.bookEntryId = bookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.timestamp = timestamp;
    }


    public String getAccountId() {
        return accountId;
    }

    public String getBookEntryId() {
        return bookEntryId;
    }

    public long getAccountTransfer() {
        return accountTransfer;
    }

    public long getBookEntryTransfer() {
        return bookEntryTransfer;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String split_exp = ";";
        sb.append(bid).append(split_exp);//0-bid
        sb.append(pid).append(split_exp);//1-pid
        sb.append(Arrays.toString(bid_array)).append(split_exp);//2-bid_array
        sb.append(num_p()).append(split_exp);//3 number of p
        sb.append("DepositEvent").append(split_exp);//4 input_event type
        sb.append(getAccountId()).append(split_exp);//5 accountId
        sb.append(getBookEntryId()).append(split_exp);//6 bookId
        sb.append(getAccountTransfer()).append(split_exp);//7 accountTransfer
        sb.append(getBookEntryTransfer()).append(split_exp);//8 bookTransfer
        sb.append(timestamp).append(split_exp);//9-timestamp
        sb.append(isAbort);//10-isAbort
        return sb.toString();
    }

    @Override
    public DepositEvent cloneEvent() {
        return new DepositEvent((int) bid, pid, Arrays.toString(bid_array),number_of_partitions,accountId,bookEntryId,accountTransfer,bookEntryTransfer,timestamp,isAbort);
    }
}
