package applications.events.ob;

import applications.events.TxnEvent;
import engine.table.tableRecords.SchemaRecordRef;

import java.util.Arrays;
import java.util.SplittableRandom;

import static UserApplications.constants.OnlineBidingSystemConstants.Constant.MAX_TOP_UP;

public class ToppingEvent extends TxnEvent {
    //place-rangeMap.
    public SchemaRecordRef[] record_refs;
    public boolean topping_result=true;

    private int[] itemId;
    private long[] itemTopUp;
    private final int num_access;

    /**
     * Creates a new ToppingEvent.
     */
    public ToppingEvent(
            int num_access, int[] itemId,
            SplittableRandom rnd,
            int partition_id, long[] bid_array, long bid, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);

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
                        int num_access, String key_array, String top_array,long timestamp) {
        super(bid, partition_id, bid_array, number_of_partitions);
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
        itemTopUp[access_id] = rnd.nextLong(MAX_TOP_UP);
    }

    public int[] getItemId() {
        return itemId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
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
        return "ToppingEvent {"
                + "itemId=" + Arrays.toString(itemId)
                + ", itemTopUp=" + Arrays.toString(itemTopUp)
                + '}';
    }
}
