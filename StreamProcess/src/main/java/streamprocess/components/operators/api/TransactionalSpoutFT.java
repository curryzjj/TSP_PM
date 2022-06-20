package streamprocess.components.operators.api;

import System.tools.SortHelper;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.TxnEvent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.InFlightLog.MultiStreamInFlightLog;
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
    protected InputDataGenerator inputDataGenerator;
    protected Queue<List<TxnEvent>> inputQueue;
    protected long previous_bid=-1;
    protected long epoch_size=0;
    protected double target_Hz;
    protected volatile int control=0;
    protected int element=0;
    protected ArrayList<String> array;
    protected boolean startClock=false;

    protected long lastSnapshotOffset;
    protected long AlignMarkerId;
    protected HashMap<Long,MultiStreamInFlightLog.BatchEvents> recoveryEvents;
    protected List<Integer> recoveryIDs = new ArrayList<>();
    protected List<Integer> downExecutorIds = new ArrayList<>();

    boolean rt = false;
    protected int total_children_tasks=0;
    protected int tthread;
    protected long start_time;
    protected long time_Interval;//ms

    //TODO:BufferedWrite

    protected int taskId;
    protected int ccOption;
    protected long bid=0;
    volatile boolean earilier_check=true;
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
        if( bid % batch_number_per_wm==0){
            return true;
        }else {
            return false;
        }
    }
    public boolean snapshot(){
        if(Time_Control){
            if(System.currentTimeMillis()-start_time>=time_Interval){
                this.start_time=System.currentTimeMillis();
                return true;
            }else {
                return false;
            }
        }else {
            if(bid%(checkpoint_interval*batch_number_per_wm)==0){
                return true;
            }else {
                return false;
            }
        }
    }
    public void forward_marker(int sourceId, long bid, Marker marker, String msg) throws InterruptedException{
        forward_marker(sourceId,DEFAULT_STREAM_ID,bid,marker,msg);
    }
    @Override
    public void forward_marker(int sourceTask, String streamId, long bid, Marker marker, String msg) throws InterruptedException {
        if (this.marker()) {//emit marker tuple
            if(enable_snapshot){
                if(snapshot()){
                    msg = "snapshot";
                    checkpoint_counter ++;
                }
            }
            if (msg.equals("snapshot") || enable_wal) {
                this.getContext().getFTM().spoutRegister(bid);
            }
            LOG.info(executor.getOP_full() + " emit " + msg + " of: " + myiteration + " @" + DateTime.now() + " SOURCE_CONTROL: " + bid);
            boardcast_time = System.nanoTime();
            collector.create_marker_boardcast(boardcast_time, streamId, bid, myiteration, msg);
            if(enable_upstreamBackup) {
                if (msg.equals("snapshot")) {
                    multiStreamInFlightLog.addEpoch(bid, DEFAULT_STREAM_ID);
                }
                multiStreamInFlightLog.addBatch(bid, DEFAULT_STREAM_ID);
            }
            myiteration++;
            success = false;
            epoch_size = bid - previous_bid;
            previous_bid = bid;
        }
    }
    public void registerRecovery() throws InterruptedException {
        this.lock = this.getContext().getFTM().getLock();
        synchronized (lock){
            this.getContext().getFTM().boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Recovery);
            try {
                if (enable_upstreamBackup){
                    if (enable_align_wait) {
                        this.collector.cleanAll();
                    } else {
                        for (int ID:recoveryIDs){
                            this.collector.clean(DEFAULT_STREAM_ID,ID);
                        }
                    }
                    this.loadInFlightLog();
                } else{
                    this.collector.cleanAll();
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
    protected abstract TxnEvent replayInputFromSSD();
    protected abstract void replayInput() throws InterruptedException;


    @Override
    public void ack_marker(Marker marker) {
        success = true;
        long elapsed_time = System.nanoTime() - boardcast_time;//the time elapsed for the system to handle the previous epoch.
        double actual_system_throughput = epoch_size * 1E9 / elapsed_time;//events/ s
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
            inputDataGenerator.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Spout sent marker "+ myiteration);
        LOG.info("Spout sent snapshot " + checkpoint_counter);
        context.stop_running();
    }

    @Override
    public void recoveryInput(long offset, List<Integer> recoveryPartitionIds, long alignOffset) throws FileNotFoundException, InterruptedException {
        this.needWaitReplay = true;
        this.replay = true;
        this.lastSnapshotOffset = offset;
        this.AlignMarkerId = alignOffset;
        if (enable_upstreamBackup) {
            Map<TopologyComponent, Grouping> children = this.executor.operator.getChildrenOfStream(DEFAULT_STREAM_ID);
            for (TopologyComponent child:children.keySet()){
                downExecutorIds.addAll(child.getExecutorIDList());
            }
            for (int partitionId:recoveryPartitionIds) {
                recoveryIDs.add(downExecutorIds.get(partitionId));
            }
        } else {
            recoveryIDs = recoveryPartitionIds;
        }
    }

    @Override
    public void cleanEpoch(long offset) {
        if (enable_upstreamBackup){
            multiStreamInFlightLog.cleanEpoch(offset,DEFAULT_STREAM_ID);
        }
    }

    @Override
    public void loadInFlightLog() {
        int ID = recoveryIDs.get(0);
        recoveryEvents = multiStreamInFlightLog.getInFlightEvents(DEFAULT_STREAM_ID, graph.getExecutionNode(ID).getOP(), lastSnapshotOffset);
    }

    @Override
    public void replayEvents() throws InterruptedException {
        ArrayList<Long> checkpointIds = SortHelper.sortKey(this.recoveryEvents.keySet());
        for (long checkpointId: checkpointIds){
            MultiStreamInFlightLog.BatchEvents batchEvents = recoveryEvents.get(checkpointId);
            if (checkpointId > lastSnapshotOffset){
                LOG.info(executor.getOP_full() + " emit " + "snapshot @" + DateTime.now() + " SOURCE_CONTROL: " + checkpointId);
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
                if (enable_align_wait && markerIds < AlignMarkerId) {
                    while(replay) {
                        int targetId = getTargetID(currentID,false);
                        if (iterators.get(targetId).hasNext()){
                            Object o = iterators.get(targetId).next();
                            collector.emit_single(DEFAULT_STREAM_ID,((TxnEvent)o).getBid(),o);
                        } else {
                            flag ++;
                            if (flag == recoveryIDs.size()){
                                replay = false;
                            }
                        }
                        currentID ++;
                        if (currentID == recoveryIDs.size()) {
                            currentID = 0;
                        }
                    }
                } else {
                    while (replay) {
                        int targetId = getTargetID(currentID, true);
                        if (iterators.get(targetId).hasNext()){
                            Object o = iterators.get(targetId).next();
                            collector.emit_single(DEFAULT_STREAM_ID,((TxnEvent)o).getBid(),o);
                        } else {
                            flag ++;
                            if (flag == downExecutorIds.size()){
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

    @Override
    public void earlier_ack_marker(Marker marker) {

    }

    @Override
    public void cleanup() {

    }
}
