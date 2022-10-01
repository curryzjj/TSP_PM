package applications.events.ob;

import applications.events.TxnEvent;
import engine.table.tableRecords.SchemaRecordRef;

import java.util.Arrays;
import java.util.SplittableRandom;

import static UserApplications.constants.OnlineBidingSystemConstants.Constant.MAX_TOP_UP;

public class ToppingEvent extends TxnEvent {
    //place-rangeMap.
    public SchemaRecordRef[] record_refs;
    public boolean topping_result = true;

    private int[] itemId;
    private long[] itemTopUp;
    private final int num_access;

    /**
     * Creates a new ToppingEvent.
     */
    public ToppingEvent(
            int num_access, int[] itemId,
            SplittableRandom rnd,
            int partition_id, long[] bid_array, long bid, int number_of_partitions,boolean isAbort) {
        super(bid, partition_id, bid_array, number_of_partitions,isAbort);

        record_refs = new SchemaRecordRef[num_access];
        this.num_access = num_access;
        for (int i = 0; i < num_access; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.itemId = itemId;
        this.timestamp=System.nanoTime();
        setValues(num_access, rnd);
    }


    /**
     * @param bid
     * @param bid_array
     * @param partition_id
     * @param number_of_partitions
     * @param num_access
     * @param key_array
     * @param top_array
     */
    public ToppingEvent(int bid, String bid_array, int partition_id, int number_of_partitions,
                        int num_access, String key_array, String top_array,long timestamp, boolean isAbort) {
        super(bid, partition_id, bid_array, number_of_partitions,isAbort);
        this.num_access = num_access;

        record_refs = new SchemaRecordRef[num_access];
        for (int i = 0; i < num_access; i++) {
            record_refs[i] = new SchemaRecordRef();
        }

        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.itemId = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.itemId[i] = Integer.parseInt(key_arrays[i].trim());
        }

        String[] top_arrays = top_array.substring(1, top_array.length() - 1).split(",");
        this.itemTopUp = new long[top_arrays.length];

        for (int i = 0; i < top_arrays.length; i++) {
            this.itemTopUp[i] = Long.parseLong(top_arrays[i].trim());
        }
        this.timestamp=timestamp;
    }

    public int getNum_access() {
        return num_access;
    }

    private void setValues(int num_access, SplittableRandom rnd) {
        itemTopUp = new long[num_access];
        for (int access_id = 0; access_id < num_access; ++access_id) {
            set_values(access_id, rnd);
        }
    }

    private void set_values(int access_id, SplittableRandom rnd) {
        if (isAbort) {
            itemTopUp[access_id] = -1;
        } else {
            itemTopUp[access_id] = rnd.nextLong(MAX_TOP_UP);
        }
    }

    public int[] getItemId() {
        return itemId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void UpdateTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------

    public long[] getItemTopUp() {
        return itemTopUp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String split_exp = ";";
        sb.append(bid).append(split_exp);//0-bid
        sb.append(pid).append(split_exp);//1-pid
        sb.append(Arrays.toString(bid_array)).append(split_exp);//2-bid_array
        sb.append(num_p()).append(split_exp);//3 number of p
        sb.append("ToppingEvent").append(split_exp);//4 input_event type
        sb.append(getNum_access()).append(split_exp);//5-number of access
        sb.append(Arrays.toString(getItemId())).append(split_exp);//6 keys int
        sb.append(Arrays.toString(getItemTopUp())).append(split_exp);//7 top up
        sb.append(timestamp).append(split_exp);//8-timestamp
        sb.append(isAbort);//9-isAbort
        return sb.toString();
    }

    @Override
    public ToppingEvent cloneEvent() {
        return new ToppingEvent((int) bid, Arrays.toString(bid_array),pid,number_of_partitions,num_access,Arrays.toString(itemId),Arrays.toString(itemTopUp),timestamp,isAbort);
    }
}
