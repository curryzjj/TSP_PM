package applications.events;

import engine.transaction.TxnContext;

import java.util.Arrays;

public class TxnEvent implements Comparable<TxnEvent> {
    protected final long bid;//as msg id
    protected final int pid;
    protected long[] bid_array;
    public int[] partition_indexes;

    protected final int number_of_partitions;
    public double[] enqueue_time = new double[1];
    public boolean[] success;
    protected long timestamp;
    protected boolean isAbort;
    public TxnContext txnContext;

    public TxnEvent(long bid, int pid, long[] bid_array, int number_of_partitions, boolean isAbort) {
        this.bid = bid;
        this.pid = pid;
        this.bid_array = bid_array;
        this.isAbort = isAbort;
        this.number_of_partitions = number_of_partitions;
        success = new boolean[1];
        success[0] = true;
    }
    public TxnEvent(long bid, int partition_id, String bid_array, int number_of_partitions, boolean isAbort) {
        this.bid = bid;
        this.pid = partition_id;
        this.isAbort = isAbort;
        String[] bid_arrays = bid_array.substring(1, bid_array.length() - 1).split(",");
        this.bid_array = new long[bid_arrays.length];

        for (int i = 0; i < bid_arrays.length; i++) {
            this.bid_array[i] = Long.parseLong(bid_arrays[i].trim());
        }
        this.number_of_partitions = number_of_partitions;
        success = new boolean[1];
        success[0] = true;
    }
    public long getBid() {
        return bid;
    }

    public int getPid() {
        return pid;
    }

    public int num_p() {
        return number_of_partitions;
    }

    public long[] getBid_array() {
        return bid_array;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void UpdateTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isAbort() {
        return isAbort;
    }

    public void setTxnContext(TxnContext txnContext) {
        this.txnContext = txnContext;
    }

    public TxnEvent cloneEvent() {
        return new TxnEvent(bid,pid, Arrays.toString(bid_array),number_of_partitions,isAbort);
    }
    public void setBid_array(String bid_array, String partition_index) {
        String[] bid_arrays = bid_array.substring(1, bid_array.length() - 1).split(",");
        this.bid_array = new long[bid_arrays.length];
        for (int i = 0; i < bid_arrays.length; i++) {
            this.bid_array[i] = Long.parseLong(bid_arrays[i].trim());
        }
        String[] partition_indexes = partition_index.substring(1, partition_index.length() - 1).split(",");
        this.partition_indexes = new int[partition_indexes.length];
        for (int i = 0; i < partition_indexes.length; i++) {
            this.partition_indexes[i] = Integer.parseInt(partition_indexes[i].trim());
        }
    }
    /**
     *
     * @param event
     * @return
     */
    @Override
    public int compareTo(TxnEvent event) {
        return Long.compare(this.bid, event.bid);
    }
}
