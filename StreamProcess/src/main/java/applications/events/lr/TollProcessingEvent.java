package applications.events.lr;

import applications.events.TxnEvent;
import engine.table.tableRecords.SchemaRecordRef;

import java.util.Arrays;

public class TollProcessingEvent extends TxnEvent {
    private final SchemaRecordRef[] speed_value;
    private final SchemaRecordRef[] count_value;
    private final int[] keys;
    private final int segmentId;
    private final int speedValue;
    public double[] roadSpendValues;
    public int[] cntValues;
    public double toll;
    private final int Vid;
    public TollProcessingEvent(int[] keys, int numAccess, long bid, int partition_id, int spendValue, int vid, long[] bid_array, int number_of_partitions, boolean isAbort) {
        super(bid, partition_id, bid_array,number_of_partitions,isAbort);
        this.timestamp = System.nanoTime();
        this.keys = keys;
        speed_value = new SchemaRecordRef[numAccess];
        count_value = new SchemaRecordRef[numAccess];
        roadSpendValues = new double[numAccess];
        cntValues = new int[numAccess];
        for (int i = 0; i < numAccess; i++) {
            count_value[i] = new SchemaRecordRef();
            speed_value[i] = new SchemaRecordRef();
        }
        this.speedValue = spendValue;
        this.segmentId = keys[0];
        this.Vid = vid;
    }
    public TollProcessingEvent(long bid, int partition_id, String bid_array, int number_of_partitions, String key_array, int spendValue, int vid, long timestamp, boolean isAbort) {
        super(bid, partition_id, bid_array,number_of_partitions,isAbort);
        this.timestamp = timestamp;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
        speed_value = new SchemaRecordRef[keys.length];
        count_value = new SchemaRecordRef[keys.length];
        roadSpendValues = new double[keys.length];
        cntValues = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            count_value[i] = new SchemaRecordRef();
            speed_value[i] = new SchemaRecordRef();
        }
        this.speedValue = spendValue;
        this.segmentId = keys[0];
        this.Vid = vid;
    }

    public int[] getKeys() {
        return keys;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getSpeedValue() {
        return speedValue;
    }

    public SchemaRecordRef[] getSpeed_value() {
        return speed_value;
    }

    public SchemaRecordRef[] getCount_value() {
        return count_value;
    }

    public int getVid() {
        return Vid;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String split_exp = ";";
        sb.append(bid).append(split_exp);//0-bid
        sb.append(pid).append(split_exp);//1-pid
        sb.append(Arrays.toString(bid_array)).append(split_exp);//2-bid_array
        sb.append(number_of_partitions).append(split_exp);//3-number_of_partition
        sb.append("TollProcessEvent").append(split_exp);//4-input_event type
        sb.append(Arrays.toString(keys)).append(split_exp);//5-keys
        sb.append(speedValue).append(split_exp);//6-SpeedValue
        sb.append(Vid).append(split_exp);//7-Vid
        sb.append(timestamp).append(split_exp);//8-timestamp
        sb.append(isAbort);//9-isAbort
        return sb.toString();
    }

    @Override
    public TollProcessingEvent cloneEvent() {
        return new TollProcessingEvent(bid, pid, Arrays.toString(bid_array),number_of_partitions,Arrays.toString(keys),speedValue,Vid,timestamp,isAbort);
    }
}
