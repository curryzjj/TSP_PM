package applications.events.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.FastZipfGenerator;
import System.tools.ZipfGenerator;
import System.tools.randomNumberGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.DataTypes.PositionReport;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.TxnEvent;
import applications.events.lr.LRParam;
import applications.events.lr.TollProcessingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static UserApplications.CONTROL.*;
import static UserApplications.CONTROL.enable_wal;

public class TPDataGenerator extends InputDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TPDataGenerator.class);
    private static final long serialVersionUID = 8318425786880652277L;

    /**
     * Generate TP data in batch
     * @param batch
     * @return
     */
    public List<TxnEvent> generateEvent(int batch){
        List<TxnEvent> batch_event = new ArrayList<>();
        if(recordNum==0){
            return null;
        }
        for(int i = 0; i < Math.min(recordNum, batch); i++){
            TollProcessingEvent event = (TollProcessingEvent) this.create_new_event(current_bid);
            batch_event.add(event);
        }
        recordNum = recordNum - Math.min(recordNum,batch);
        return batch_event;
    }

    @Override
    public void generateEvent() {
        for(int i = 0; i < recordNum; i++){
            TollProcessingEvent event= (TollProcessingEvent) this.create_new_event(current_bid);
            events.add(event);
        }
    }

    @Override
    public void storeInput(Object input) throws IOException {
        List<TxnEvent> txnEvents = (List<TxnEvent>) input;
        File file = new File(dataPath);
        FileWriter Fw = null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for (TxnEvent event:txnEvents){
            TollProcessingEvent tollProcessingEvent = (TollProcessingEvent) event;
            String str = tollProcessingEvent.toString();
            bw.write(str+"\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public List<AbstractInputTuple> generateData(int batch) {
       return null;
    }

    public void initialize(String dataPath, Configuration config){
        super.initialize(dataPath,config);
    }

    @Override
    public Object create_new_event(int bid) {
        LRParam param = new LRParam(NUM_ACCESSES);
        Set<Integer> keys = new HashSet<>();
        current_pid = key_to_partition(partitionId_generator.next());
        randomKeys(param, keys, NUM_ACCESSES);
        assert !enable_states_partition|| verify(keys,current_pid, partition_num);
        current_bid++;
        if (random.nextInt(1000) < RATIO_OF_ABORT) {
            return new TollProcessingEvent(param.getSegmentIds(),NUM_ACCESSES, bid, current_pid, randomNumberGenerator.generateRandom(180,200),randomNumberGenerator.generateRandom(1,100),p_bid,partition_num,true);
        } else {
            return new TollProcessingEvent(param.getSegmentIds(),NUM_ACCESSES, bid, current_pid, randomNumberGenerator.generateRandom(60,180),randomNumberGenerator.generateRandom(1,100),p_bid,partition_num,false);
        }
    }

    @Override
    public void close() {
        LocalFileSystem fs = new LocalFileSystem();
        try {
            fs.delete(new Path(dataPath),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
