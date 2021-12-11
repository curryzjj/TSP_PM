package applications.events.gs;

import applications.events.TxnEvent;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.IntDataBox;
import engine.table.datatype.DataBoxImpl.StringDataBox;
import engine.table.tableRecords.SchemaRecordRef;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;


import java.util.ArrayList;
import java.util.List;

import static UserApplications.CONTROL.NUM_ACCESSES;
import static UserApplications.constants.GrepSumConstants.Constant.VALUE_LEN;

public class MicroEvent extends TxnEvent {
    private final SchemaRecordRef[] recordRefs;
    private final int[] keys;
    private final boolean flag;
    private List<DataBox>[] value;
    public int sum;
    public int result[]=new int[NUM_ACCESSES];

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
            , int partition_id, long[] bid_array, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.timestamp=System.nanoTime();
        this.flag = flag;
        this.keys = keys;
        recordRefs = new SchemaRecordRef[numAccess];
        for (int i = 0; i < numAccess; i++) {
            recordRefs[i] = new SchemaRecordRef();
        }
        setValues(keys);
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
                      String key_array, boolean flag,long timestamp ) {
        super(bid, pid, bid_array, num_of_partition);
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
        setValues(keys);
        this.timestamp=timestamp;
    }
    public int[] getKeys() {
        return keys;
    }

    public List<DataBox>[] getValues() {
        return value;
    }

    public SchemaRecordRef[] getRecord_refs() {
        return recordRefs;
    }


    public boolean READ_EVENT() {
        return flag;
    }
    private static String rightpad(@NotNull String text, int length) {
        return StringUtils.rightPad(text, length); // Returns "****foobar"
//        return String.format("%-" + length + "." + length + "s", text);
    }
    public static String GenerateValue(int key) {
        return rightpad(String.valueOf(key), VALUE_LEN);
    }
    public void setValues(int[] keys) {
        value = new ArrayList[NUM_ACCESSES];//Note, it should be arraylist instead of linkedlist as there's no add/remove later.
        for (int access_id = 0; access_id < NUM_ACCESSES; ++access_id) {
            set_values(access_id, keys[access_id]);
        }
    }
    private void set_values(int access_id, int key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));//key  4 bytes
        values.add(new StringDataBox(GenerateValue(key), VALUE_LEN));//value_list   32 bytes..
        value[access_id] = values;
    }
}
