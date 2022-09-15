package applications.events.InputDataStore.ImplDataStore;

import applications.events.InputDataStore.InputStore;
import applications.events.TxnEvent;
import applications.events.gs.MicroEvent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GSInputStore extends InputStore {
    private static final long serialVersionUID = -1620241688153093878L;
    @Override
    public void storeInput(List<TxnEvent> inputs) throws IOException {
        File file = new File(inputFile.concat(inputStorePaths.get(currentOffset)));
        FileWriter Fw= null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for(TxnEvent e:inputs){
            MicroEvent microEvent = (MicroEvent) e;
            String str = microEvent.getBid()+//0--bid long
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
                    Arrays.toString(microEvent.getValues())+//5 keys int
                    split_exp+
                    microEvent.READ_EVENT()+//6 is read_event boolean
                    split_exp+
                    microEvent.getTimestamp()+//7 timestamp long
                    split_exp+
                    microEvent.isAbort();//8 abort
            bw.write(str+"\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public void close() {

    }
}
