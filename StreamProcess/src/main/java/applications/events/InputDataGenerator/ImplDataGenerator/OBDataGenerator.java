package applications.events.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.FastZipfGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.TxnEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.OBParam;
import applications.events.ob.ToppingEvent;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static UserApplications.CONTROL.*;
import static UserApplications.CONTROL.NUM_ITEMS;
import static UserApplications.constants.OnlineBidingSystemConstants.Constant.*;

public class OBDataGenerator extends InputDataGenerator {
    private final static Logger LOG=LoggerFactory.getLogger(OBDataGenerator.class);
    protected int[] event_decision_distribute;
    protected int event_decision_id;
    @Override
    public List<AbstractInputTuple> generateData(int batch) {
        return null;
    }

    @Override
    public List<TxnEvent> generateEvent(int batch) {
        List<TxnEvent> batch_event=new ArrayList<>();
        if(recordNum==0){
            return null;
        }
        for(int i=0;i<Math.min(recordNum,batch);i++){
            TxnEvent event= (TxnEvent) this.create_new_event(current_bid);
            batch_event.add(event);
        }
        if(enable_snapshot||enable_wal){
            try {
                storeInput(batch_event);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        recordNum=recordNum-Math.min(recordNum,batch);
        return batch_event;
    }

    @Override
    public void storeInput(Object input) throws IOException {
        List<TxnEvent> inputs= (List<TxnEvent>) input;
        File file=new File(dataPath);
        FileWriter Fw= null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for(TxnEvent event:inputs){
            StringBuilder sb = new StringBuilder();
            if (event instanceof BuyingEvent) {
                sb.append(((BuyingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).num_p());//3
                sb.append(split_exp);
                sb.append("BuyingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getItemId()));//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidPrice()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidQty()));//7
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).getTimestamp());

            } else if (event instanceof AlertEvent) {
                sb.append(((AlertEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((AlertEvent) event).num_p());
                sb.append(split_exp);
                sb.append("AlertEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getAsk_price()));
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getTimestamp());

            } else if (event instanceof ToppingEvent) {
                sb.append(((ToppingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).num_p());
                sb.append(split_exp);
                sb.append("ToppingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemTopUp()));
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getTimestamp());
            }
            bw.write(sb.toString() + "\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public void initialize(String dataPath, int recordNum, int range, double zipSkew, Configuration config) {
        this.recordNum=recordNum;
        this.dataPath=dataPath;
        this.zipSkew=zipSkew;
        this.range=range;
        this.current_pid=0;
        this.event_decision_id=0;
        this.partition_num =config.getInt("partition_num");
        if(enable_states_partition){
            floor_interval= (int) Math.floor(NUM_ITEMS / (double) partition_num);//NUM_ITEMS / partition_num;
            partitioned_store =new FastZipfGenerator[partition_num];
            for (int i = 0; i < partition_num; i++) {
                partitioned_store[i] = new FastZipfGenerator((int) floor_interval, zipSkew, i * floor_interval);
            }
        }else{
            this.shared_store=new FastZipfGenerator(NUM_ITEMS, zipSkew,0);
        }
        p_bid = new long[partition_num];

        for (int i = 0; i < partition_num; i++) {
            p_bid[i] = 0;
        }
        if (RATIO_OF_READ == 0) {
            this.event_decision_distribute=new int[]{1,2,1,2,1,2,1,2};//alert,topping
        } else if (RATIO_OF_READ == 0.25) {
            this.event_decision_distribute=new int[]{0,0,1,2,1,2,1,2};//2:3:3 buy, alert, topping
        } else if (RATIO_OF_READ == 0.5) {
            this.event_decision_distribute=new int[]{0,0,0,0,1,2,1,2};//2:1:1 buy, alert, topping
        } else if (RATIO_OF_READ == 0.75) {
            this.event_decision_distribute=new int[]{0,0,0,0,0,0,1,2};//6:1:1 buy, alert, topping
        } else if (RATIO_OF_READ == 1) {
            this.event_decision_distribute=new int[]{0,0,0,0,0,0,0,0};//8:0:0 never used
        } else {
            throw new UnsupportedOperationException();
        }
        LOG.info("ratio_of_read: " + RATIO_OF_READ + "\tREAD DECISIONS: " + Arrays.toString(read_decision));
    }

    @Override
    public Object create_new_event(int bid) {
        int flag = next_decision();
        if (flag == 0) {
            return randomBuyEvents(p_bid.clone(), bid, rnd);
        } else if (flag == 1) {
            return randomAlertEvents(p_bid.clone(), bid, rnd);//(AlertEvent) in.getValue(0);
        } else {
            return randomToppingEvents(p_bid.clone(), bid, rnd);//(AlertEvent) in.getValue(0);
        }
    }
    protected int next_decision() {
        int rt = event_decision_distribute[event_decision_id];
        event_decision_id++;
        if (event_decision_id == 8)
            event_decision_id = 0;
        return rt;
    }
    @Override
    public void close() {
        LocalFileSystem fs=new LocalFileSystem();
        try {
            fs.delete(new Path(dataPath),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private BuyingEvent randomBuyEvents(long[] bid_array, long bid, SplittableRandom rnd){
        OBParam param=new OBParam(NUM_ACCESSES_PER_BUY);
        Set keys=new HashSet();
        int counter=0;
        int access_per_partition=(int) Math.ceil(NUM_ACCESSES_PER_BUY / (double) partition_num);
        randomKeys(current_pid,param,keys,access_per_partition,counter,NUM_ACCESSES_PER_BUY);
        assert !enable_states_partition || verify(keys, current_pid, partition_num);
        current_bid++;
        return new BuyingEvent(param.keys(),rnd,current_pid,bid_array,bid, partition_num);
    }
    private AlertEvent randomAlertEvents(long[] bid_array, long bid, SplittableRandom rnd){
        int num_access=rnd.nextInt(NUM_ACCESSES_PER_ALERT)+5;
        OBParam param=new OBParam(num_access);
        Set keys=new HashSet();
        int counter=0;
        int access_per_partition = (int) Math.ceil(num_access / (double) partition_num);
        randomKeys(current_pid,param,keys,access_per_partition,counter,num_access);
        assert verify(keys, current_pid, partition_num);
        current_bid++;
        return new AlertEvent(num_access,param.keys(),rnd,current_pid,bid_array,bid, partition_num);
    }
    private ToppingEvent randomToppingEvents(long[] bid_array, long bid, SplittableRandom rnd){
        int num_access=rnd.nextInt(NUM_ACCESSES_PER_TOP)+5;
        OBParam param=new OBParam(num_access);
        Set keys=new HashSet();
        int counter=0;
        int access_per_partition = (int) Math.ceil(num_access / (double) partition_num);
        randomKeys(current_pid,param,keys,access_per_partition,counter,num_access);
        assert verify(keys, current_pid, partition_num);
        current_bid++;
        return new ToppingEvent(num_access,param.keys(),rnd,current_pid,bid_array,bid, partition_num);
    }
}
