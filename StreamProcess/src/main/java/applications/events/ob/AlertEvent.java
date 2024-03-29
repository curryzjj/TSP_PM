package applications.events.ob;

import applications.events.TxnEvent;
import engine.table.tableRecords.SchemaRecordRef;

import java.util.Arrays;
import java.util.SplittableRandom;

import static UserApplications.constants.OnlineBidingSystemConstants.Constant.MAX_Price;

public class AlertEvent extends TxnEvent {
    private final int num_access;
    public boolean alert_result = true;
    private int[] itemId;//keys.
    private long[] ask_price;//new ask price
    public SchemaRecordRef[] record_refs;
    public AlertEvent(int num_access, int[] itemId, SplittableRandom rnd,int partition_id,long[] bid_array,long bid,int number_of_partitions,boolean isAbort){
        super(bid,partition_id,bid_array,number_of_partitions,isAbort);
        this.num_access = num_access;
        record_refs=new SchemaRecordRef[num_access];
        for (int i=0;i<num_access;i++){
            record_refs[i]=new SchemaRecordRef();
        }
        this.timestamp=System.nanoTime();
        this.itemId=itemId;
        setValues(num_access,rnd);
    }

    /**
     * load an AlertEvent
     * @param bid
     * @param bid_array
     * @param partition_id
     * @param number_of_partitions
     * @param num_access
     * @param key_array
     * @param alert_array
     */
    public AlertEvent(int bid, String bid_array, int partition_id, int number_of_partitions,
                      int num_access, String key_array, String alert_array,long timestamp,boolean isAbort) {
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

        String[] top_arrays = alert_array.substring(1, alert_array.length() - 1).split(",");
        this.ask_price = new long[top_arrays.length];

        for (int i = 0; i < top_arrays.length; i++) {
            this.ask_price[i] = Long.parseLong(top_arrays[i].trim());
        }
        this.timestamp = timestamp;
    }
    private void setValues(int num_access,SplittableRandom rnd){
        ask_price = new long[num_access];
        for (int access_id=0;access_id<num_access;++access_id){
            if (isAbort) {
                ask_price[access_id] = -1;
            } {
                ask_price[access_id] = rnd.nextLong(MAX_Price);
            }
        }
    }

    public int[] getItemId() {
        return itemId;
    }

    public long[] getAsk_price() {
        return ask_price;
    }

    public int getNum_access() {
        return num_access;
    }
    public void UpdateTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String split_exp = ";";
        sb.append(bid).append(split_exp);//0-bid
        sb.append(pid).append(split_exp);//1-pid
        sb.append(Arrays.toString(bid_array)).append(split_exp);//2-bid_array
        sb.append(num_p()).append(split_exp);//3 number of p
        sb.append("AlertEvent").append(split_exp);//4 input_event type
        sb.append(getNum_access()).append(split_exp);//5-number of access
        sb.append(Arrays.toString(getItemId())).append(split_exp);//6 keys int
        sb.append(Arrays.toString(getAsk_price())).append(split_exp);//6 ask_price
        sb.append(timestamp).append(split_exp);//8-timestamp
        sb.append(isAbort);//9-isAbort
        return sb.toString();
    }

    @Override
    public AlertEvent cloneEvent() {
        return new AlertEvent((int) bid, Arrays.toString(bid_array),pid,number_of_partitions,num_access,Arrays.toString(itemId),Arrays.toString(ask_price),timestamp,isAbort);
    }
}
