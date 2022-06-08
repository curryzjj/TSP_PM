package applications.events.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
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
    private static final long serialVersionUID = 8857832060697651311L;

    @Override
    public List<AbstractInputTuple> generateData(int batch) {
        return null;
    }

    @Override
    public List<TxnEvent> generateEvent(int batch) {
        List<TxnEvent> batch_event=new ArrayList<>();
        if(recordNum == 0){
            return null;
        }
        for(int i=0;i<Math.min(recordNum,batch);i++){
            MicroEvent microEvent= (MicroEvent) this.create_new_event(current_bid);
            batch_event.add(microEvent);
        }
        recordNum = recordNum - Math.min(recordNum,batch);
        return batch_event;
    }

    @Override
    public void storeInput(Object input) throws IOException {
        List<TxnEvent> inputs = (List<TxnEvent>) input;
        File file=new File(dataPath);
        FileWriter Fw= null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for(TxnEvent e:inputs){
            MicroEvent microEvent= (MicroEvent) e;
            String str=microEvent.getBid()+//0--bid long
                    split_exp+
                    microEvent.getPid()+//1--pid int
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
        super.initialize(dataPath,recordNum,range,zipSkew,config);
    }

    @Override
    public Object create_new_event(int bid) {
        boolean flag = next_decision();
        MicroParam param = new MicroParam(NUM_ACCESSES);
        Set<Integer> keys = new HashSet<>();
        current_pid = key_to_partition(partitionId_generator.next());
        randomKeys(param, keys, NUM_ACCESSES);
        assert !enable_states_partition|| verify(keys,current_pid, partition_num);
        current_bid++;
        return new MicroEvent(param.getKeys(), flag, NUM_ACCESSES, bid, current_pid, p_bid, partition_num);
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
