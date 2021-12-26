package applications.spout.transactional;

import System.constants.BaseConstants;
import System.util.OsUtils;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.gs.MicroEvent;
import applications.events.TxnEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.TransactionalSpoutFT;
import streamprocess.execution.ExecutionGraph;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import static System.Constants.Mac_Data_Path;
import static System.Constants.Node22_Data_Path;
import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public class EventSpoutWithFT extends TransactionalSpoutFT {
    private static final Logger LOG= LoggerFactory.getLogger(SpoutWithFT.class);
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
            this.exe=TEST_NUM_EVENTS;
            Data_path=Mac_Data_Path;
        }else{
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
            this.exe=NUM_EVENTS;
            Data_path=Node22_Data_Path;
        }
        this.batch_number_per_wm=config.getInt("batch_number_per_wm");
        this.checkpoint_interval = config.getInt("snapshot");
        Data_path = Data_path.concat(path);
        inputDataGenerator.initialize(Data_path,this.exe,NUM_ITEMS-1,ZIP_SKEW,config);
        this.getContext().getEventGenerator().setInputDataGenerator(inputDataGenerator);
        this.inputQueue=this.getContext().getEventGenerator().getEventsQueue();
        this.getContext().getEventGenerator().start();
    }

    @Override
    public void nextTuple(int batch) throws InterruptedException, IOException {
        if (needReplay){
            this.registerRecovery();
        }
        if(replay){
            TxnEvent event=replayEvent();
            if(event!=null){
                collector.emit_single(DEFAULT_STREAM_ID,bid,event);
                bid++;
                lostData++;
                forward_marker(this.taskId, bid, null,"marker");
            }else{
                collector.create_marker_boardcast(boardcast_time, DEFAULT_STREAM_ID, bid, myiteration,"recovery");
            }
        }else{
            List<TxnEvent> events= (List<TxnEvent>) inputQueue.poll();
            while(events==null){
                events=(List<TxnEvent>) inputQueue.poll();
            }
            if(events.size()!=0){
                if(enable_snapshot||enable_clr||enable_wal){
                    this.inputDataGenerator.storeInput(events);
                }
                for (TxnEvent input : events) {
                    collector.emit_single(DEFAULT_STREAM_ID, bid, input);
                    bid++;
                    forward_marker(this.taskId, bid, null, "marker");
                }
            }else{
                stopRunning();
            }
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
                            Boolean.parseBoolean(split[6]),//flag
                            Long.parseLong(split[7])
                    );
                    break;
                case "BuyingEvent":
                    event=new BuyingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], //bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//key_array
                            split[6],//price_array
                            split[7]  ,//qty_array
                            Long.parseLong(split[8])
                    );
                    break;
                case "AlertEvent":
                    event = new AlertEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], // bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7],//price_array
                            Long.parseLong(split[8])
                    );
                    break;
                case "ToppingEvent":
                    event = new ToppingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], Integer.parseInt(split[1]), //pid
                            //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7] , //top_array
                            Long.parseLong(split[8])
                    );
                    break;
                case "DepositEvent":
                    event = new DepositEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//getAccountId
                            split[6],//getBookEntryId
                            Integer.parseInt(split[7]),  //getAccountTransfer
                            Integer.parseInt(split[8]),  //getBookEntryTransfer
                            Long.parseLong(split[9])
                    );
                    break;
                case "TransactionEvent":
                    event = new TransactionEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//getSourceAccountId
                            split[6],//getSourceBookEntryId
                            split[7],//getTargetAccountId
                            split[8],//getTargetBookEntryId
                            Integer.parseInt(split[9]),  //getAccountTransfer
                            Integer.parseInt(split[10]),  //getBookEntryTransfer
                            Long.parseLong(split[11])
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
