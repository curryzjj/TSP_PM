package streamprocess.components.operators.api;

import System.measure.MeasureTools;
import System.tools.SortHelper;
import UserApplications.CONTROL;
import applications.events.InputDataStore.InputStore;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.TxnEvent;
import applications.events.gs.MicroEvent;
import applications.events.lr.TollProcessingEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.InFlightLog.MultiStreamInFlightLog;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.FailureFlag;
import streamprocess.faulttolerance.FaultToleranceConstants;
import streamprocess.faulttolerance.checkpoint.emitMarker;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class TransactionalSpoutFT extends AbstractSpout implements emitMarker {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalSpoutFT.class);
    private static final long serialVersionUID = 7501673262740197416L;
    protected InputStore inputStore;
    protected Queue<List<TxnEvent>> inputQueue;
    protected long previous_bid=-1;
    protected long epoch_size=0;
    protected volatile int control=0;

    protected long lastSnapshotOffset = -1;
    protected long AlignMarkerId;
    protected long storedOffset = -1;
    protected HashMap<Long,MultiStreamInFlightLog.BatchEvents> recoveryEvents;
    protected List<Integer> recoveryIDs = new ArrayList<>();
    protected List<Integer> downExecutorIds = new ArrayList<>();
    protected Queue<Long> storedSnapshotOffsets = new ArrayDeque<>();

    protected int tthread;
    protected long snapshotRecordTime;
    protected long time_Interval;//ms
    //TODO:BufferedWrite

    protected int taskId;
    protected long bid = 0;
    protected boolean earlier_finish = false;
    public int empty=0;

    protected int batch_number_per_wm;
    protected int checkpoint_interval;
    protected int checkpoint_counter=0;
    protected MultiStreamInFlightLog multiStreamInFlightLog;

    protected TransactionalSpoutFT(Logger log) {
        super(log);
    }

    public double getEmpty(){
        return empty;
    }
    @Override
    public abstract void nextTuple(int batch) throws InterruptedException, IOException;
    public boolean marker(){
        return bid % batch_number_per_wm == 0;
    }
    public boolean snapshot(){
        if(Time_Control){
            if(System.currentTimeMillis() - snapshotRecordTime >= time_Interval || checkpoint_counter == 0){
                this.snapshotRecordTime = System.currentTimeMillis();
                return true;
            }else {
                return false;
            }
        }else {
            return bid % (checkpoint_interval * batch_number_per_wm) == 0;
        }
    }
    public void forward_marker(int sourceId, long bid, Marker marker, String msg) throws InterruptedException{
        forward_marker(sourceId,DEFAULT_STREAM_ID,bid,marker,msg);
    }
    @Override
    public void forward_marker(int sourceTask, String streamId, long bid, Marker marker, String msg) throws InterruptedException {
        if (this.marker()) {
            if(enable_snapshot){
                if (replay) {
                    if (bid == storedOffset) {
                        msg = "snapshot";
                    }
                } else {
                    if(snapshot()){
                        msg = "snapshot";
                        checkpoint_counter ++;
                        this.inputStore.switchInputStorePath(bid);
                    }
                }
            }
            if (msg.equals("snapshot") || enable_wal) {
                this.getContext().getFTM().spoutRegister(bid);
            }
            LOG.info(executor.getOP_full() + " emit " + msg + " of: " + myiteration + " @" + DateTime.now() + " SOURCE_CONTROL: " + bid);
            boardcast_time = System.nanoTime();
            collector.create_marker_boardcast(boardcast_time, streamId, bid, myiteration, msg);
            if(enable_spoutBackup) {
                if (msg.equals("snapshot")) {
                    multiStreamInFlightLog.addEpoch(bid, DEFAULT_STREAM_ID);
                }
                multiStreamInFlightLog.addBatch(bid, DEFAULT_STREAM_ID);
            }
            myiteration ++;
            epoch_size = bid - previous_bid;
            previous_bid = bid;
        }
    }
    public void registerRecovery() throws InterruptedException {
        this.lock = this.getContext().getFTM().getLock();
        synchronized (lock){
            this.getContext().getFTM().boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Recovery);
            try {
                if (enable_clr){
                    if (enable_align_wait) {
                        this.collector.cleanAll();
                    } else {
                        for (int ID:recoveryIDs){
                            this.collector.clean(DEFAULT_STREAM_ID, ID);
                        }
                    }
                } else{
                    this.collector.cleanAll();
                }
                if (enable_spoutBackup) {
                    this.loadInFlightLog();
                } else {
                    this.loadInputFromSSD();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            lock.notifyAll();
        }
        synchronized (lock){
            while (!isCommit){
                LOG.info(this.executor.getOP_full()+" is waiting for the Recovery");
                lock.wait();
            }
            this.isCommit = false;
            this.needWaitReplay = false;
        }
    }

    protected abstract void loadInputFromSSD() throws FileNotFoundException;
    protected abstract TxnEvent replayInputFromSSD() throws FileNotFoundException;
    protected abstract void replayInput() throws InterruptedException, FileNotFoundException;


    @Override
    public void ack_Signal(Tuple message) {

    }
    public void stopRunning() throws InterruptedException {
        if(enable_wal|| enable_checkpoint ||enable_clr){
            this.getContext().getFTM().spoutRegister(bid);
        }
        LOG.info(executor.getOP_full() + " emit " + "finish" + " of: " + myiteration + " @" + DateTime.now() + " SOURCE_CONTROL: " + bid);
        boardcast_time = System.nanoTime();
        collector.create_marker_boardcast(boardcast_time, DEFAULT_STREAM_ID, bid, myiteration, "finish");
        try {
            clock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Spout sent marker "+ myiteration);
        LOG.info("Spout sent snapshot " + checkpoint_counter);
        this.earlier_finish = true;
    }

    @Override
    public void recoveryInput(long offset, List<Integer> recoveryPartitionIds, long alignOffset) throws FileNotFoundException, InterruptedException {
        this.needWaitReplay = true;
        this.lastSnapshotOffset = offset;
        this.AlignMarkerId = alignOffset;
        if (enable_spoutBackup) {
            Map<TopologyComponent, Grouping> children = this.executor.operator.getChildrenOfStream(DEFAULT_STREAM_ID);
            for (TopologyComponent child:children.keySet()){
                downExecutorIds.addAll(child.getExecutorIDList());
            }
            for (int partitionId:recoveryPartitionIds) {
                recoveryIDs.add(downExecutorIds.get(partitionId));
            }
        } else {
            recoveryIDs = new ArrayList<>(recoveryPartitionIds);
        }
    }

    @Override
    public void cleanEpoch(long offset) {
        if (enable_spoutBackup){
            multiStreamInFlightLog.cleanEpoch(offset,DEFAULT_STREAM_ID);
        }
    }

    @Override
    public void loadInFlightLog() {
        MeasureTools.Input_load_begin(System.nanoTime());
        this.replay = true;
        int ID = recoveryIDs.get(0);
        recoveryEvents = multiStreamInFlightLog.getInFlightEvents(DEFAULT_STREAM_ID, graph.getExecutionNode(ID).getOP(), lastSnapshotOffset);
        MeasureTools.Input_load_finish(System.nanoTime());
    }

    @Override
    public void replayEvents() throws InterruptedException {
        MeasureTools.ReExecute_time_begin(System.nanoTime());
        ArrayList<Long> checkpointIds = SortHelper.sortKey(this.recoveryEvents.keySet());
        for (long checkpointId: checkpointIds){
            MultiStreamInFlightLog.BatchEvents batchEvents = recoveryEvents.get(checkpointId);
            if (checkpointId > lastSnapshotOffset){
                LOG.info(executor.getOP_full() + " emit " + "snapshot @" + DateTime.now() + " SOURCE_CONTROL: " + checkpointId);
                if (Time_Control){
                    this.snapshotRecordTime = System.currentTimeMillis();
                }
                this.FTM.spoutRegister(checkpointId);
                collector.create_marker_boardcast(System.nanoTime(), DEFAULT_STREAM_ID, checkpointId, myiteration,"snapshot");
            }
            for (long markerIds : batchEvents.getMarkerId()){
                replay = true;
                int currentID = 0;
                int flag = 0;
                ConcurrentHashMap<Integer, Iterator<Object>> iterators = batchEvents.getBatchEvents(markerIds);
                if (markerIds != checkpointId && markerIds > lastSnapshotOffset){
                    LOG.info(executor.getOP_full() + " emit " + "marker @" + DateTime.now() + " SOURCE_CONTROL: " + markerIds);
                    collector.create_marker_boardcast(System.nanoTime(),DEFAULT_STREAM_ID, markerIds, myiteration,"marker");
                }
                HashMap<Integer, Boolean> hasFinish = new HashMap<>();
                if (enable_align_wait && markerIds < AlignMarkerId) {
                    for (int id : recoveryIDs) {
                        hasFinish.put(id, false);
                    }
                    while(replay) {
                        int targetId = getTargetID(currentID,false);
                        if (iterators.get(targetId).hasNext()){
                            TxnEvent o = deserializeEvent((String) iterators.get(targetId).next());
                            collector.emit_single(DEFAULT_STREAM_ID, o.getBid(),o);
                        } else {
                            hasFinish.put(targetId, true);
                            if (!hasFinish.containsValue(false)){
                                replay = false;
                            }
                        }
                        currentID ++;
                        if (currentID == recoveryIDs.size()) {
                            currentID = 0;
                        }
                    }
                } else {
                    for (int id : downExecutorIds) {
                        hasFinish.put(id, false);
                    }
                    while (replay) {
                        int targetId = getTargetID(currentID, true);
                        if (iterators.get(targetId).hasNext()){
                            TxnEvent o = deserializeEvent((String) iterators.get(targetId).next());
                            collector.emit_single(DEFAULT_STREAM_ID, o.getBid(),o);
                        } else {
                            hasFinish.put(targetId, true);
                            if (!hasFinish.containsValue(false)){
                                replay = false;
                            }
                        }
                        currentID ++;
                        if (currentID == downExecutorIds.size()) {
                            currentID = 0;
                        }
                    }
                }
            }
        }
    }

    public int getTargetID(int currentID, boolean isAlign){
        if (isAlign) {
            return downExecutorIds.get(currentID);
        } else {
            return recoveryIDs.get(currentID);
        }
    }

    public TxnEvent deserializeEvent(String eventString) {
        TxnEvent event;
        String[] split = eventString.split(";");
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
    }

    @Override
    public void earlier_ack_marker(Marker marker) {
    }

    @Override
    public void cleanup() {
    }
}
