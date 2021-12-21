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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static UserApplications.CONTROL.*;
import static UserApplications.CONTROL.enable_wal;

public class TPDataGenerator extends InputDataGenerator {
    private static final Logger LOG=LoggerFactory.getLogger(TPDataGenerator.class);
    private String dataPath;
    private ZipfGenerator zipfGenerator;

    /**
     * Generate TP data in batch and store in the input store
     * @param batch
     * @return
     */
    public List<TxnEvent> generateEvent(int batch){
        return null;
    }

    @Override
    public void storeInput(Object input) throws IOException {
        List<AbstractInputTuple> inputTuples= (List<AbstractInputTuple>) input;
        File file=new File(dataPath);
        FileWriter Fw= null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for (AbstractInputTuple tuple:inputTuples){
            PositionReport report=(PositionReport) tuple;
            String str=report.getTime()
                    +split_exp+report.getVid()
                    +split_exp+report.getSpeed()
                    +split_exp+report.getXWay()
                    +split_exp+report.getLane()
                    +split_exp+report.getDirection()
                    +split_exp+report.getSegment()
                    +split_exp+report.getPosition();
            bw.write(str+"\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public List<AbstractInputTuple> generateData(int batch) {
        List<AbstractInputTuple> batch_event=new ArrayList<>();
        if(recordNum==0){
            return null;
        }
        for(int i=0;i<Math.min(recordNum,batch);i++){
            PositionReport report= (PositionReport) this.create_new_event(current_bid);
            batch_event.add(report);
        }
        if (enable_snapshot||enable_wal||enable_clr){
            try {
                storeInput(batch_event);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        recordNum=recordNum-Math.min(recordNum,batch);
        return batch_event;
    }

    public void initialize(String dataPath, int recordNum, int range, double zipSkew, Configuration config){
        this.recordNum=recordNum;
        this.dataPath=dataPath;
        this.zipSkew=zipSkew;
        this.range=range;
        this.partition_num =config.getInt("partition_num");
        this.zipfGenerator=new ZipfGenerator(range, zipSkew);
        this.current_pid=0;
        if(enable_states_partition){
            floor_interval= (int) Math.floor(NUM_ITEMS / (double) partition_num);//NUM_ITEMS / partition_num;
            partitioned_store =new FastZipfGenerator[partition_num];
            for (int i = 0; i < partition_num; i++) {
                partitioned_store[i] = new FastZipfGenerator((int) floor_interval, zipSkew, i * floor_interval);
            }
        }else{
            this.shared_store=new FastZipfGenerator(NUM_ITEMS, zipSkew,0);
        }
    }

    @Override
    public Object create_new_event(int bid) {
        FastZipfGenerator generator;
        if(enable_states_partition){
            generator= partitioned_store[current_pid];
        }else {
            generator=shared_store;
        }
        long timestamp = System.nanoTime();
        current_pid++;
        if(current_pid== partition_num){
            current_pid=0;
        }
        return new PositionReport(timestamp,
                randomNumberGenerator.generateRandom(1,100),
                randomNumberGenerator.generateRandom(60,180),
                randomNumberGenerator.generateRandom(1,4),
                (short)randomNumberGenerator.generateRandom(1,4),
                (short)randomNumberGenerator.generateRandom(1,1),
                generator.next(),
                randomNumberGenerator.generateRandom(1,100));
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
