package applications.events.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.FastZipfGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.MicroEvent;
import applications.events.MicroParam;
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
        File file=new File(dataPath);
        FileWriter Fw= null;
        try {
            Fw = new FileWriter(file,true);
            BufferedWriter bw= new BufferedWriter(Fw);
            for(int i=0;i<Math.min(recordNum,batch);i++){
                MicroEvent microEvent= (MicroEvent) this.create_new_event(i);
                batch_event.add(microEvent);
                String str=microEvent.getBid()+//0--bid
                        split_exp+
                        microEvent.getPid()+//1
                        split_exp+
                        Arrays.toString(microEvent.getBid_array())+//2
                        split_exp+
                        microEvent.num_p() +//3 num of p
                        split_exp +
                        "MicroEvent"+//4 input_event type
                        split_exp+
                        Arrays.toString(microEvent.getKeys())+//5 keys
                        split_exp+
                        microEvent.READ_EVENT();//6 is read_event
                bw.write(str+"\n");
                bw.flush();
            }
            bw.close();
            Fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        recordNum=recordNum-Math.min(recordNum,batch);
        return batch_event;
    }

    @Override
    public void initialize(String dataPath, int recordNum, int range, double zipSkew, Configuration config) {
        this.recordNum=recordNum;
        this.dataPath=dataPath;
        this.zipSkew=zipSkew;
        this.range=range;
        this.current_pid=0;
        this.read_decision_id=0;
        this.tthread=config.getInt("tthread");
        this.access_per_partition = (int) Math.ceil(NUM_ACCESSES / (double) tthread);
        if(enable_states_partition){
            floor_interval= (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
            partitioned_store =new FastZipfGenerator[tthread];
            for (int i = 0; i < tthread; i++) {
                partitioned_store[i] = new FastZipfGenerator((int) floor_interval, zipSkew, i * floor_interval);
            }
        }else{
            this.shared_store=new FastZipfGenerator(NUM_ITEMS, zipSkew,0);
        }
        p_bid = new long[tthread];

        for (int i = 0; i < tthread; i++) {
            p_bid[i] = 0;
        }
        if (RATIO_OF_READ == 0) {
            read_decision = new boolean[]{false, false, false, false, false, false, false, false};// all write.
        } else if (RATIO_OF_READ == 0.25) {
            read_decision = new boolean[]{false, false, false, false, false, false, true, true};//75% W, 25% R.
        } else if (RATIO_OF_READ == 0.5) {
            read_decision = new boolean[]{false, false, false, false, true, true, true, true};//equal r-w ratio.
        } else if (RATIO_OF_READ == 0.75) {
            read_decision = new boolean[]{false, false, true, true, true, true, true, true};//25% W, 75% R.
        } else if (RATIO_OF_READ == 1) {
            read_decision = new boolean[]{true, true, true, true, true, true, true, true};// all read.
        } else {
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
        assert !enable_states_partition|| verify(keys,current_pid,tthread);
        return new MicroEvent(param.getKeys(),flag,NUM_ACCESSES,bid,current_pid,p_bid,tthread);
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
