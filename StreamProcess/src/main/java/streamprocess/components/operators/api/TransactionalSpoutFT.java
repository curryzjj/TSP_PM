package streamprocess.components.operators.api;

import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.TxnEvent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.MultiStreamInFlightLog;
import streamprocess.faulttolerance.FaultToleranceConstants;
import streamprocess.faulttolerance.checkpoint.emitMarker;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class TransactionalSpoutFT extends AbstractSpout implements emitMarker {
    private static final Logger LOG= LoggerFactory.getLogger(TransactionalSpoutFT.class);
    protected InputDataGenerator inputDataGenerator;
    protected Queue inputQueue;
    protected long previous_bid=-1;
    protected long epoch_size=0;
    protected double target_Hz;
    protected volatile int control=0;
    protected int element=0;
    protected ArrayList<String> array;
    protected boolean startClock=false;
    protected long offset;
    protected HashMap<Integer,Iterator<Object>> recoveryEvents = new HashMap<>();
    protected List<Integer> recoveryIDs = new ArrayList<>();
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
        if(bid%batch_number_per_wm==0){
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
        String msg1=msg;
        if (this.marker()) {//emit marker tuple
            if(enable_snapshot){
                if(snapshot()){
                    if(this.getContext().getFTM().spoutRegister(bid)){
                        msg1="snapshot";
                        checkpoint_counter++;
                    }
                }
            } else if (enable_wal) {
                if(this.getContext().getFTM().spoutRegister(bid)){
                    if (snapshot()){
                        msg1="snapshot";
                        checkpoint_counter++;
                    }
                }
            }else if(enable_clr){
                if (!this.getContext().getFTM().spoutRegister(bid)){
                    return;
                }
            }
            LOG.info(executor.getOP_full() + " emit marker of: " + myiteration + " @" + DateTime.now() + " SOURCE_CONTROL: " + bid);
            boardcast_time = System.nanoTime();
            collector.create_marker_boardcast(boardcast_time, streamId, bid, myiteration,msg1);
            if(enable_upstreamBackup) {
                multiStreamInFlightLog.addEvent(streamId,new Marker(streamId,boardcast_time,bid,myiteration,msg1));
                if (msg1.equals("snapshot")) {
                    multiStreamInFlightLog.addEpoch(bid, DEFAULT_STREAM_ID);
                } else if (enable_wal) {
                    multiStreamInFlightLog.addEpoch(bid, DEFAULT_STREAM_ID);
                }
            }
            myiteration++;
            success = false;
            epoch_size = bid - previous_bid;
            previous_bid = bid;
        }
    }
    public void registerRecovery() throws InterruptedException {
        this.lock=this.getContext().getFTM().getLock();
        synchronized (lock){
            this.getContext().getFTM().boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Recovery);
            try {
                if (enable_upstreamBackup){
                    for (int ID:recoveryIDs){
                        this.collector.clean(DEFAULT_STREAM_ID,ID);
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
            this.isCommit =false;
            this.needWaitReplay =false;
        }
    }

    protected abstract void loadInputFromSSD() throws FileNotFoundException;
    protected abstract TxnEvent replayInputFromSSD();
    protected abstract void replayInput() throws InterruptedException;


    @Override
    public void ack_marker(Marker marker) {
        success=true;
        long elapsed_time = System.nanoTime() - boardcast_time;//the time elapsed for the system to handle the previous epoch.
        double actual_system_throughput = epoch_size * 1E9 / elapsed_time;//events/ s
    }
    public void stopRunning() throws InterruptedException {
        if(enable_wal||enable_snapshot||enable_clr){
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

    @Override
    public void recoveryInput(long offset, List<Integer> recoveryExecutorIds) throws FileNotFoundException, InterruptedException {
        this.needWaitReplay =true;
        this.replay=true;
        this.offset=offset;
        if (recoveryExecutorIds != null){
           recoveryIDs=recoveryExecutorIds;
        } else {
          Map<TopologyComponent, Grouping> children=this.executor.operator.getChildrenOfStream(DEFAULT_STREAM_ID);
          for (TopologyComponent child:children.keySet()){
              for (int ID : child.getExecutorIDList()){
                  recoveryIDs.add(ID);
              }
          }
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
        for (int ID:recoveryIDs){
            recoveryEvents.put(ID,multiStreamInFlightLog.getInFightEventsForExecutor(DEFAULT_STREAM_ID,graph.getExecutionNode(ID).getOP(),ID,offset).iterator());
        }
    }

    @Override
    public void replayEvents() throws InterruptedException {
        int currentID=0;
        while (replay) {
            int targetId = getTargetID(currentID);
            if (targetId !=-1){
                Object o = getInFlightLog(targetId);
                if (o!=null){
                    if (o instanceof Marker){
                        collector.single_marker(DEFAULT_STREAM_ID,((Marker) o).msgId,targetId, (Marker) o);
                    } else {
                        collector.emit_single_ID(DEFAULT_STREAM_ID,targetId,((TxnEvent)o).getBid(),o);
                    }
                    currentID ++;
                    if (currentID == recoveryIDs.size()){
                        currentID = 0;
                    }
                }
            }
        }
    }

    public int getTargetID(int currentID){
        if (recoveryIDs.size() == 0){
            replay = false;
            return -1;
        } else {
            return recoveryIDs.get(currentID);
        }
    }
    public Object getInFlightLog(int targetID) {
        if(recoveryEvents.get(targetID).hasNext()){
            return recoveryEvents.get(targetID).next();
        } else {
            recoveryIDs.removeIf(id -> id.intValue() == targetID);
            return null;
        }
    }

    @Override
    public void earlier_ack_marker(Marker marker) {

    }

    @Override
    public void cleanup() {

    }
}
