package applications.spout.transactional;

import System.constants.BaseConstants;
import System.measure.MeasureTools;
import System.util.OsUtils;
import applications.events.InputDataStore.InputStore;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.gs.MicroEvent;
import applications.events.TxnEvent;
import applications.events.lr.TollProcessingEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.TransactionalSpoutFT;
import streamprocess.controller.output.InFlightLog.MultiStreamInFlightLog;
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
    private static final Logger LOG= LoggerFactory.getLogger(EventSpoutWithFT.class);
    private static final long serialVersionUID = 5206772865951921120L;
    private Scanner scanner;
    private String Data_path;
    public EventSpoutWithFT(){
        super(LOG);
        this.scalable=false;
        status = new Status();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("EventSpoutWithFT initialize is being called");
        cnt = 0;
        counter = 0;
        this.graph = graph;
        taskId = getContext().getThisTaskId();
        String OS_prefix="";
        String path;
        Data_path = "";
        if(OsUtils.isWindows()){
            OS_prefix="win.";
        }else{
            OS_prefix="unix.";
        }
        if(OsUtils.isMac()){
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
            Data_path = Mac_Data_Path;
        }else{
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
            Data_path = Node22_Data_Path;
        }
        this.exe = NUM_EVENTS;
        this.batch_number_per_spout = config.getInt("batch_number_per_wm") / config.getInt("spoutThread");
        this.checkpoint_interval = config.getInt("snapshot") / config.getInt("spoutThread");
        int batch_number_per_wm = config.getInt("batch_number_per_wm");
        for (long i = batch_number_per_wm; i <= exe; i = i + batch_number_per_wm) {
            this.markerIds.add(i);
        }
        Data_path = Data_path.concat(path);
        inputStore.initialize(Data_path);
        this.inputQueue = this.getContext().getEventGenerator().getEventsQueue(thisTaskId);
        this.start_time = System.currentTimeMillis();
        this.time_Interval = config.getInt("time_Interval");
        if (enable_upstreamBackup){
            multiStreamInFlightLog = new MultiStreamInFlightLog(this.executor.operator);
        }
    }

    @Override
    public void nextTuple(int batch) throws InterruptedException, IOException {
        if (needWaitReplay){
            this.registerRecovery();
            if (enable_upstreamBackup) {
                replayEvents();
            } else{
                replayInput();
            }
            if (earlier_finish) {
                stopRunning();
            }
        } else if (!earlier_finish){
            List<TxnEvent> events = inputQueue.poll();
            while(events == null){
                events = inputQueue.poll();
            }
            if(events.size() != 0){
                if(enable_input_store){
                    MeasureTools.Input_store_begin(System.nanoTime());
                    this.inputStore.storeInput(events);
                    MeasureTools.Input_store_finish();
                }
                if (enable_measure) {
                    MeasureTools.SetWaitTime((System.nanoTime() - events.get(0).getTimestamp()) / 1E6);
                }
                for (TxnEvent input : events) {
                    int targetId = collector.emit_single(DEFAULT_STREAM_ID, input.getBid(), input);
                    count ++;
                    if (enable_upstreamBackup) {
                        MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
                        //TODO: clone the input
                        TxnEvent event = input.cloneEvent();
                        multiStreamInFlightLog.addEvent(event.getPid(), DEFAULT_STREAM_ID, event);
                        MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
                    }
                    forward_marker(this.taskId, input.getBid(), null, "marker");
                }
                if (enable_upstreamBackup) {
                    MeasureTools.Upstream_backup_finish_acc(this.executor.getExecutorID());
                }
            }else{
                stopRunning();
            }
        }
    }



    @Override
    protected void loadInputFromSSD() throws FileNotFoundException {
        long msg = lastSnapshotOffset;
        count = lastSnapshotOffset / this.executor.operator.getNumTasks();
        this.storedSnapshotOffsets.addAll(this.inputStore.getStoredSnapshotOffsets(lastSnapshotOffset));
        openFile(Data_path.concat(this.inputStore.getInputStorePath(storedSnapshotOffsets.poll())));
        long eachAlignNumber = this.AlignMarkerId / this.executor.operator.getNumTasks() - count;
        while (eachAlignNumber != 0){
            scanner.nextLine();
            count ++;
            eachAlignNumber --;
        }
        LOG.info("The input data have been load to the offset " + this.AlignMarkerId);
    }

    @Override
    protected void replayInput() throws InterruptedException, FileNotFoundException {
        MeasureTools.ReExecute_time_begin(System.nanoTime());
        while(replay) {
            TxnEvent event = replayInputFromSSD();
            if (event != null) {
                if (!enable_clr || count >= AlignMarkerId || recoveryIDs.contains(event.getPid())) {
                    collector.emit_single(DEFAULT_STREAM_ID, count, event);
                    lostData ++;
                }
                count++;
                forward_marker(this.taskId, count, null, "marker");
            }
        }
    }


    @Override
    protected TxnEvent replayInputFromSSD() throws FileNotFoundException {
        if(scanner.hasNextLine()){
            TxnEvent event;
            String read = scanner.nextLine();
            String[] split = read.split(";");
            switch (split[4]){
                case "MicroEvent":
                    event = new MicroEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//key_array
                            Boolean.parseBoolean(split[6]),//flag
                            Long.parseLong(split[7]),//timestamp
                            Boolean.parseBoolean(split[8])//isAbort
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
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
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
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
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
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
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
                            Long.parseLong(split[9]),
                            Boolean.parseBoolean(split[10])
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
                            Long.parseLong(split[11]),
                            Boolean.parseBoolean(split[12])
                    );
                    break;
                case "TollProcessEvent" :
                    event = new TollProcessingEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],
                            Integer.parseInt(split[6]),
                            Integer.parseInt(split[7]),
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
                    );
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + split[4]);
            }
            return event;
        }else{
            if (storedSnapshotOffsets.size() != 0){
                openFile(Data_path.concat(this.inputStore.getInputStorePath(storedSnapshotOffsets.poll())));
                return replayInputFromSSD();
            } else {
                scanner.close();
                LOG.info("The number of lost data is " + lostData);
                replay = false;
                return null;
            }
        }
    }
    private void openFile(String fileName) throws FileNotFoundException {
        scanner = new Scanner(new File(fileName), "UTF-8");
    }
    @Override
    public void setInputStore(InputStore inputStore) {
        this.inputStore = inputStore;
    }
}
