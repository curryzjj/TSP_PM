package streamprocess.components.operators.api;

import System.tools.FastZipfGenerator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.checkpoint.Checkpointable;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.util.ArrayList;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.enable_debug;

public abstract class TransactionalSpout extends AbstractSpout implements Checkpointable {
    private static final Logger LOG= LoggerFactory.getLogger(TransactionalSpout.class);
    protected transient FastZipfGenerator keygenerator;
    protected long previous_bid=-1;
    protected long epoch_size=0;
    protected double target_Hz;
    protected double checkpoint_interval_sec;
    protected volatile int control=0;
    protected int element=0;
    protected ArrayList<String> array;
    protected int counter=0;
    protected boolean startClock=false;


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
    protected TransactionalSpout(Logger log) {
        super(log);
    }

    public double getEmpty(){
        return empty;
    }
    @Override
    public abstract void nextTuple() throws InterruptedException;
    public boolean checkpoint(int counter){
        if(counter%batch_number_per_wm==0){
            return true;
        }else {
            return false;
        }
    }
    public void forward_checkpoint(int sourceId, long bid, Marker marker,String msg) throws InterruptedException{
        forward_checkpoint(sourceId,DEFAULT_STREAM_ID,bid,marker,msg);
    }
    public void forward_checkpoint_single(int sourceId,long bid,Marker marker) throws InterruptedException{
        forward_checkpoint_single(sourceId,DEFAULT_STREAM_ID,bid,marker);
    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException {
        if (clock.tick(myiteration) && success&&this.getContext().getFTM().spoutRegister(bid)) {//emit marker tuple
            collector.create_marker_single(boardcast_time, streamId, bid, myiteration);
            boardcast_time = System.nanoTime();
            myiteration++;
            success = false;
            epoch_size = bid - previous_bid;
            previous_bid = bid;
            earilier_check = true;
        }
    }
    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker,String msg) throws InterruptedException {
        if (clock.tick(myiteration) && success&&this.getContext().getFTM().spoutRegister(bid)) {//emit marker tuple
            LOG.info(executor.getOP_full() + " emit marker of: " + myiteration + " @" + DateTime.now() + " SOURCE_CONTROL: " + bid);
            collector.create_marker_boardcast(boardcast_time, streamId, bid, myiteration,msg);
            boardcast_time = System.nanoTime();
            myiteration++;
            success = false;
            epoch_size = bid - previous_bid;
            previous_bid = bid;
            earilier_check = true;
        }
    }
    public void registerRecovery() throws InterruptedException {
        if(this.getContext().getRM().needRecovery()){
            this.getContext().getRM().spoutRegister(this.executor.getExecutorID());
            Marker marker=new Marker(DEFAULT_STREAM_ID,boardcast_time,0,myiteration,"recovery");
            this.collector.broadcast_marker(bid,marker);
            this.lock=this.getContext().getRM().getLock();
            synchronized (lock){
                while (!isCommit){
                    LOG.info(this.executor.getOP_full()+" is waiting for the Recovery");
                    lock.wait();
                }
            }
        }else{
            isCommit=true;
        }
    }
    @Override
    public void ack_checkpoint(Marker marker) {
        success=true;
        if(enable_debug){
            //LOG.trace("task_size: " + epoch_size * NUM_ACCESSES);
        }
        long elapsed_time = System.nanoTime() - boardcast_time;//the time elapsed for the system to handle the previous epoch.
        double actual_system_throughput = epoch_size * 1E9 / elapsed_time;//events/ s
    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }

    @Override
    public void cleanup() {

    }
}
