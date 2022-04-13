package applications.events.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.FastZipfGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.gs.MicroEvent;
import applications.events.gs.MicroParam;
import applications.events.TxnEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static UserApplications.CONTROL.*;

public class GSDataGenerator extends InputDataGenerator {
    private static final Logger LOG=LoggerFactory.getLogger(GSDataGenerator.class);
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
            MicroEvent microEvent= (MicroEvent) this.create_new_event(current_bid);
            batch_event.add(microEvent);
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
        for(TxnEvent e:inputs){
            MicroEvent microEvent= (MicroEvent) e;
            String str=microEvent.getBid()+//0--bid long
                    split_exp+
                    microEvent.getPid()+//1 int
                    split_exp+
                    Arrays.toString(microEvent.getBid_array())+//2 int
                    split_exp+
                    microEvent.num_p() +//3 num of p int
                    split_exp +
                    "MicroEvent"+//4 input_event type
                    split_exp+
                    Arrays.toString(microEvent.getKeys())+//5 keys int
                    split_exp+
                    microEvent.READ_EVENT()+//6 is read_event boolean
                    split_exp+
                    microEvent.getTimestamp();//7 timestamp long
            bw.write(str+"\n");
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
        this.read_decision_id=0;
        this.partition_num =config.getInt("partition_num");
        this.partition_num_per_txn=config.getInt("partition_num_per_txn");
        this.access_per_partition = (int) Math.ceil(NUM_ACCESSES / (double) partition_num_per_txn);
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
            read_decision = new boolean[]{false,false,false, false, false, false, false, false, false, false};// all write.
        } else if (RATIO_OF_READ == 0.1) {
            read_decision = new boolean[]{false,false,false, false, false, false, false, false, false, true};//90% W, 10% R.
        } else if (RATIO_OF_READ == 0.2) {
            read_decision = new boolean[]{false,false,false, false, false, false, false, false, true, true};//80% W, 20% R.
        } else if (RATIO_OF_READ == 0.3) {
            read_decision = new boolean[]{false,false,false, false, false, false, false, true, true, true};//70% W, 30% R.
        } else if (RATIO_OF_READ == 0.4) {
            read_decision = new boolean[]{false,false,false, false, false, false, true, true, true, true};//60% W, 40% R.
        } else if (RATIO_OF_READ==0.5){
            read_decision = new boolean[]{false,false,false, false, false, true, true, true, true, true};//50% W, 50% R.
        }else if (RATIO_OF_READ==0.6){
            read_decision = new boolean[]{false,false,false, false, true, true, true, true, true, true};//40% W, 60% R.
        }else if (RATIO_OF_READ==0.7){
            read_decision = new boolean[]{false,false,false, true, true, true, true, true, true, true};//30% W, 70% R.
        }else if (RATIO_OF_READ==0.8){
            read_decision = new boolean[]{false,false,true, true, true, true, true, true, true, true};//20% W, 80% R.
        }else if (RATIO_OF_READ==0.9){
            read_decision = new boolean[]{false,true,true, true, true, true, true, true, true, true};//10% W, 90% R.
        }else if (RATIO_OF_READ==1.0){
            read_decision = new boolean[]{true,true,true, true, true, true, true, true, true, true};// all read.
        } else{
            throw new UnsupportedOperationException();
        }
        LOG.info("ratio_of_read: " + RATIO_OF_READ + "\tREAD DECISIONS: " + Arrays.toString(read_decision));
    }

    @Override
    public Object create_new_event(int bid) {
        boolean flag=next_read_decision();
        MicroParam param=new MicroParam(NUM_ACCESSES);
        int counter=0;
        Set keys = new HashSet();
        randomKeys(current_pid,param,keys,access_per_partition,counter,NUM_ACCESSES);
        assert !enable_states_partition|| verify(keys,current_pid, partition_num);
        current_bid++;
        return new MicroEvent(param.getKeys(),flag,NUM_ACCESSES,bid,current_pid,p_bid, partition_num);
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
}
