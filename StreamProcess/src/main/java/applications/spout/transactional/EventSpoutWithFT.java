package applications.spout.transactional;

import System.constants.BaseConstants;
import System.measure.MeasureTools;
import System.util.OsUtils;
import applications.events.InputDataStore.InputStore;
import applications.events.TxnEvent;
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
        this.batch_number_per_wm = config.getInt("batch_number_per_wm");
        this.checkpoint_interval = config.getInt("snapshot");
        Data_path = Data_path.concat(path);
        inputStore.initialize(Data_path);
        time_Interval = config.getInt("time_Interval");
        this.inputQueue = this.getContext().getEventGenerator().getEventsQueue();
        if (enable_spoutBackup){
            multiStreamInFlightLog = new MultiStreamInFlightLog(this.executor.operator);
        }
    }

    @Override
    public void nextTuple(int batch) throws InterruptedException, IOException {
        if (bid == 0 && Time_Control) {
            this.start_time = System.currentTimeMillis();
        }
        if (needWaitReplay){
            this.registerRecovery();
            if (Time_Control){
                this.start_time = System.currentTimeMillis();
            }
            if (enable_spoutBackup) {
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
                    int targetId = collector.emit_single(DEFAULT_STREAM_ID, bid, input);
                    bid ++;
                    if (enable_spoutBackup) {
                        MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
                        multiStreamInFlightLog.addEvent(input.getPid(), DEFAULT_STREAM_ID, input.toString());
                        MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
                    }
                    forward_marker(this.taskId, bid, null, "marker");
                }
                if (enable_spoutBackup) {
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
        bid = lastSnapshotOffset;
        this.storedSnapshotOffsets.addAll(this.inputStore.getStoredSnapshotOffsets(lastSnapshotOffset));
        openFile(Data_path.concat(this.inputStore.getInputStorePath(storedSnapshotOffsets.poll())));
        if (storedSnapshotOffsets.size() != 0) {
            lastSnapshotOffset = storedSnapshotOffsets.poll();
        } else {
            lastSnapshotOffset = -1;
        }
        long align = this.AlignMarkerId - lastSnapshotOffset;
        this.replay = true;
        if (enable_wal) {
            while (align != 0){
                if (scanner.hasNextLine()) {
                    scanner.nextLine();
                }
                bid ++;
                align --;
            }
            LOG.info("The input data have been load to the offset " + this.AlignMarkerId);
        } else {
            LOG.info("The input data have been load to the offset " + msg);
        }
    }

    @Override
    protected void replayInput() throws InterruptedException, FileNotFoundException {
        MeasureTools.ReExecute_time_begin(System.nanoTime());
        while(replay) {
            TxnEvent event = replayInputFromSSD();
            if (event != null) {
                if (enable_clr) {
                    if (bid >= AlignMarkerId) {
                        collector.emit_single(DEFAULT_STREAM_ID, bid, event);
                    } else if (recoveryIDs.contains(event.getPid())) {
                        collector.emit_single(DEFAULT_STREAM_ID, bid, event);
                        lostData ++;
                    }
                } else {
                    collector.emit_single(DEFAULT_STREAM_ID, bid, event);
                    lostData ++;
                }
                bid ++;
                forward_marker(this.taskId, bid, null, "marker");
            }
        }
    }


    @Override
    protected TxnEvent replayInputFromSSD() throws FileNotFoundException {
        if(scanner.hasNextLine()){
            TxnEvent event;
            event = deserializeEvent(scanner.nextLine());
            return event;
        }else{
            if (lastSnapshotOffset != -1){
                openFile(Data_path.concat(this.inputStore.getInputStorePath(lastSnapshotOffset)));
                if (storedSnapshotOffsets.size() != 0) {
                    lastSnapshotOffset = storedSnapshotOffsets.poll();
                } else {
                    lastSnapshotOffset = -1;
                }
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
