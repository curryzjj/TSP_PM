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
    protected int event_decision_id = 0;
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
        recordNum = recordNum-Math.min(recordNum,batch);
        return batch_event;
    }

    @Override
    public void generateEvent() {
        for(int i = 0; i < recordNum; i++){
            TxnEvent event= (TxnEvent) this.create_new_event(current_bid);
            events.add(event);
        }
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
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).isAbort());
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
                sb.append(split_exp);
                sb.append(((AlertEvent) event).isAbort());
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
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).isAbort());
            }
            bw.write(sb.toString() + "\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public void initialize(String dataPath, Configuration config) {
       super.initialize(dataPath,config);

    }

    @Override
    public Object create_new_event(int bid) {
        int flag = next_event_decision();
        if (flag == 0) {
            return randomBuyEvents(p_bid.clone(), bid, rnd);
        } else if (flag == 1) {
            return randomAlertEvents(p_bid.clone(), bid, rnd);//(AlertEvent) in.getValue(0);
        } else {
            return randomToppingEvents(p_bid.clone(), bid, rnd);//(AlertEvent) in.getValue(0);
        }
    }
    protected int next_event_decision() {
        if (next_decision()) {
            return 0;
        } else {
            event_decision_id ++;
            if (event_decision_id > 2)
                event_decision_id = 1;
            return event_decision_id;
        }
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
        OBParam param = new OBParam(NUM_ACCESSES);
        Set<Integer> keys = new HashSet<>();
        randomKeys(param,keys,NUM_ACCESSES);
        assert !enable_states_partition || verify(keys, current_pid, partition_num);
        current_bid++;
        if (random.nextInt(1000) < RATIO_OF_ABORT) {
            return new BuyingEvent(param.keys(),rnd,current_pid,bid_array,bid, partition_num,true);
        } else {
            return new BuyingEvent(param.keys(),rnd,current_pid,bid_array,bid, partition_num,false);
        }
    }
    private AlertEvent randomAlertEvents(long[] bid_array, long bid, SplittableRandom rnd){
        OBParam param = new OBParam(NUM_ACCESSES);
        Set<Integer> keys = new HashSet<>();
        randomKeys(param,keys,NUM_ACCESSES);
        assert verify(keys, current_pid, partition_num);
        current_bid++;
        if (random.nextInt(1000) < RATIO_OF_ABORT) {
            return new AlertEvent(NUM_ACCESSES,param.keys(),rnd,current_pid,bid_array,bid, partition_num,true);
        } else {
            return new AlertEvent(NUM_ACCESSES,param.keys(),rnd,current_pid,bid_array,bid, partition_num,false);
        }
    }
    private ToppingEvent randomToppingEvents(long[] bid_array, long bid, SplittableRandom rnd){
        OBParam param=new OBParam(NUM_ACCESSES);
        Set<Integer> keys=new HashSet<>();
        randomKeys(param,keys,NUM_ACCESSES);
        assert verify(keys, current_pid, partition_num);
        current_bid++;
        if (random.nextInt(1000) < RATIO_OF_ABORT) {
            return new ToppingEvent(NUM_ACCESSES,param.keys(),rnd,current_pid,bid_array,bid, partition_num,true);
        } else {
            return new ToppingEvent(NUM_ACCESSES,param.keys(),rnd,current_pid,bid_array,bid, partition_num,false);
        }
    }
}
