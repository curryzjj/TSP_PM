package applications.spout.transactional;

import System.constants.BaseConstants;
import System.util.OsUtils;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.MicroEvent;
import applications.events.TxnEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.TransactionalSpoutFT;
import streamprocess.execution.ExecutionGraph;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import static System.Constants.Mac_Data_Path;
import static System.Constants.Node22_Data_Path;
import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public class EventSpoutWithFT extends TransactionalSpoutFT {
    private static final Logger LOG= LoggerFactory.getLogger(SpoutWithFT.class);
    private InputDataGenerator inputDataGenerator;
    private Scanner scanner;
    private String Data_path;

    public EventSpoutWithFT(){
        super(LOG);
        this.scalable=false;
        status=new Status();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("EventSpoutWithFT initialize is being called");
        cnt=0;
        counter=0;
        this.graph=graph;
        taskId= getContext().getThisTaskId();
        String OS_prefix="";
        String path;
        Data_path = "";
        if(OsUtils.isWindows()){
            OS_prefix="win.";
        }else{
            OS_prefix="unix.";
        }
        if(OsUtils.isMac()){
            path=config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
            this.exe=NUM_EVENTS;
            Data_path=Mac_Data_Path;
        }else{
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
            this.exe=TEST_NUM_EVENTS;
            Data_path=Node22_Data_Path;
        }
        this.batch_number_per_wm=config.getInt("batch_number_per_wm");
        this.checkpoint_interval = config.getInt("snapshot");
        Data_path = Data_path.concat(path);
        inputDataGenerator.initialize(Data_path,this.exe,NUM_ITEMS-1,ZIP_SKEW,config);
    }

    @Override
    public void nextTuple(int batch) throws InterruptedException {
        if (needReplay){
            this.registerRecovery();
        }
        while(replay&&batch!=0){
            TxnEvent event=replayEvent();
            if(event!=null){
                collector.emit_single(DEFAULT_STREAM_ID,bid,event);
                bid++;
                lostData++;
                batch--;
                forward_marker(this.taskId, bid, null,"marker");
            }
        }
        List<TxnEvent> events=inputDataGenerator.generateEvent(batch);
        if(events!=null){
            for (Iterator<TxnEvent> it = events.iterator(); it.hasNext(); ) {
                TxnEvent input = it.next();
                collector.emit_single(DEFAULT_STREAM_ID,bid,input);
                bid++;
                forward_marker(this.taskId, bid, null,"marker");
            }
        }else{
            if(enable_wal||enable_snapshot){
                this.getContext().getFTM().spoutRegister(bid);
            }
            collector.create_marker_boardcast(boardcast_time, DEFAULT_STREAM_ID, bid, myiteration,"finish");
            try {
                clock.close();
                inputDataGenerator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("Spout sent marker "+myiteration);
            LOG.info("Spout sent snapshot "+checkpoint_counter);
            context.stop_running();
        }

    }
    @Override
    public void recoveryInput(long offset) throws FileNotFoundException, InterruptedException {
        this.needReplay =true;
        this.replay=true;
        this.offset=offset;
    }


    @Override
    protected void loadReplay() throws FileNotFoundException {
        long msg=offset;
        bid=0;
        openFile(Data_path);
        while (offset!=0){
            scanner.nextLine();
            offset--;
            bid++;
        }
        LOG.info("The input data have been load to the offset "+msg);
    }

    @Override
    protected TxnEvent replayEvent() {
        if(scanner.hasNextLine()){
            TxnEvent event;
            String read = scanner.nextLine();
            String[] split = read.split(";");
            switch (split[4]){
                case "MicroEvent":
                    event=new MicroEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//key_array
                            Boolean.parseBoolean(split[6])//flag
                    );
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + split[4]);
            }
            return event;
        }else{
            scanner.close();
            LOG.info("The number of lost data is "+lostData);
            replay=false;
            return null;
        }
    }
    private void openFile(String fileName) throws FileNotFoundException {
        scanner = new Scanner(new File(fileName), "UTF-8");
    }
    @Override
    public void setInputDataGenerator(InputDataGenerator inputDataGenerator) {
        this.inputDataGenerator=inputDataGenerator;
    }
}
