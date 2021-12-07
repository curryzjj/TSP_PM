package streamprocess.components.operators.api;

import System.tools.FastZipfGenerator;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.TxnEvent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.FaultToleranceConstants;
import streamprocess.faulttolerance.checkpoint.emitMarker;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class TransactionalSpoutFT extends AbstractSpout implements emitMarker {
    private static final Logger LOG= LoggerFactory.getLogger(TransactionalSpoutFT.class);
    protected InputDataGenerator inputDataGenerator;
    protected long previous_bid=-1;
    protected long epoch_size=0;
    protected double target_Hz;
    protected volatile int control=0;
    protected int element=0;
    protected ArrayList<String> array;
    protected boolean startClock=false;
    protected long offset;
    boolean rt = false;
    protected int total_children_tasks=0;
    protected int tthread;

    //TODO:BufferedWrite

    protected int taskId;
    protected int ccOption;
    protected long bid=0;
    volatile boolean earilier_check=true;
    public int empty=0;

    protected int batch_number_per_wm;
    protected int checkpoint_interval;
    protected int checkpoint_counter=0;

    protected TransactionalSpoutFT(Logger log) {
        super(log);
    }

    public double getEmpty(){
        return empty;
    }
    @Override
    public abstract void nextTuple(int batch) throws InterruptedException;
    public boolean marker(){
        if(bid%batch_number_per_wm==0){
            return true;
        }else {
            return false;
        }
    }
    public boolean snapshot(){
        if(bid%(checkpoint_interval*batch_number_per_wm)==0){
            return true;
        }else {
            return false;
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
            }else if(enable_wal){
                if (!this.getContext().getFTM().spoutRegister(bid)){
                    return;
                }
            }
            LOG.info(executor.getOP_full() + " emit marker of: " + myiteration + " @" + DateTime.now() + " SOURCE_CONTROL: " + bid);
            collector.create_marker_boardcast(boardcast_time, streamId, bid, myiteration,msg1);
            boardcast_time = System.nanoTime();
            myiteration++;
            success = false;
            epoch_size = bid - previous_bid;
            previous_bid = bid;
            earilier_check = true;
        }
    }
    public void registerRecovery() throws InterruptedException {
        this.lock=this.getContext().getFTM().getLock();
        synchronized (lock){
            this.getContext().getFTM().boltRegister(this.executor.getExecutorID(), FaultToleranceConstants.FaultToleranceStatus.Recovery);
            this.collector.clean();
            Marker marker=new Marker(DEFAULT_STREAM_ID,boardcast_time,0,myiteration,"recovery");
            this.collector.broadcast_marker(bid,marker);
            try {
                this.loadReplay();
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
            this.needReplay=false;
        }
    }

    protected abstract void loadReplay() throws FileNotFoundException;
    protected abstract TxnEvent replayEvent();

    @Override
    public void ack_marker(Marker marker) {
        success=true;
        long elapsed_time = System.nanoTime() - boardcast_time;//the time elapsed for the system to handle the previous epoch.
        double actual_system_throughput = epoch_size * 1E9 / elapsed_time;//events/ s
    }
    public void stopRunning() throws InterruptedException {
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

    @Override
    public void earlier_ack_marker(Marker marker) {

    }

    @Override
    public void cleanup() {

    }
}
