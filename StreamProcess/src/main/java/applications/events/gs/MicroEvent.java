package applications.events.gs;

import applications.events.TxnEvent;
import engine.table.tableRecords.SchemaRecordRef;


import java.util.Arrays;
import java.util.SplittableRandom;

import static UserApplications.CONTROL.NUM_ACCESSES;
import static UserApplications.constants.GrepSumConstants.Constant.MAX_VALUE;

public class MicroEvent extends TxnEvent {
    private final SchemaRecordRef[] recordRefs;
    private final int[] keys;
    private final boolean flag;
    private int[] values;
    public int sum;
    public int[] result = new int[NUM_ACCESSES];

    /**
     * Create a new MicroEvent
     * @param keys
     * @param flag
     * @param numAccess
     * @param bid
     * @param partition_id
     * @param bid_array
     * @param number_of_partitions
     */
    public MicroEvent(int[] keys, boolean flag, int numAccess, long bid
            , int partition_id, long[] bid_array, int number_of_partitions , boolean isAbort, SplittableRandom rnd) {
        super(bid, partition_id, bid_array, number_of_partitions, isAbort);
        this.timestamp = System.nanoTime();
        this.flag = flag;
        this.keys = keys;
        recordRefs = new SchemaRecordRef[numAccess];
        this.values = new int[numAccess];
        for (int i = 0; i < numAccess; i++) {
            recordRefs[i] = new SchemaRecordRef();
        }
        if (!flag) {
            setValues(keys, rnd);
        }
    }
    /**
     * Loading a DepositEvent.
     *
     * @param flag,            read_write flag
     * @param bid
     * @param pid
     * @param bid_array
     * @param num_of_partition
     * @param key_array
     */
    public MicroEvent(int bid, int pid, String bid_array, int num_of_partition,
                      String key_array, String value_array, boolean flag,long timestamp, boolean isAbort) {
        super(bid, pid, bid_array, num_of_partition, isAbort);
        recordRefs = new SchemaRecordRef[NUM_ACCESSES];
        for (int i = 0; i < NUM_ACCESSES; i++) {
            recordRefs[i] = new SchemaRecordRef();
        }
        this.flag = flag;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
        if (!flag) {
            String[] value_arrays = value_array.substring(1, value_array.length() - 1).split(",");
            this.values = new int[value_arrays.length];
            for (int i = 0; i < value_arrays.length; i++) {
                this.values[i] = Integer.parseInt(value_arrays[i].trim());
            }
        }
        this.timestamp = timestamp;
    }
    public int[] getKeys() {
        return keys;
    }

    public int[] getValues() {
        return values;
    }

    public SchemaRecordRef[] getRecord_refs() {
        return recordRefs;
    }


    public boolean READ_EVENT() {
        return flag;
    }


    public void setValues(int[] keys, SplittableRandom rnd) {
        for (int access_id = 0; access_id < NUM_ACCESSES; access_id ++) {
            set_values(access_id, rnd);
        }
    }
    private void set_values(int access_id, SplittableRandom rnd) {
        if (!isAbort) {
            values[access_id] = rnd.nextInt(MAX_VALUE);
        } else {
            values[access_id] = -1;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String split_exp = ";";
        sb.append(bid).append(split_exp);//0-bid
        sb.append(pid).append(split_exp);//1-pid
        sb.append(Arrays.toString(bid_array)).append(split_exp);//2-bid_array
        sb.append(num_p()).append(split_exp);//3 number of p
        sb.append("MicroEvent").append(split_exp);//4 input_event type
        sb.append(Arrays.toString(getKeys())).append(split_exp);//5 keys int
        sb.append(Arrays.toString(getValues())).append(split_exp);//6 values int
        sb.append(flag).append(split_exp);//7-isRead
        sb.append(timestamp).append(split_exp);//8-timestamp
        sb.append(isAbort);//9-isAbort
        return sb.toString();
    }

    @Override
    public MicroEvent cloneEvent() {
        return new MicroEvent((int) bid,pid, Arrays.toString(bid_array),number_of_partitions,Arrays.toString(keys),Arrays.toString(values),flag,timestamp,isAbort);
    }
}
