package streamprocess.components.operators.api;

import System.util.OsUtils;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.TxnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.checkpoint.Checkpointable;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import utils.SOURCE_CONTROL;

import java.util.concurrent.BrokenBarrierException;

import static UserApplications.CONTROL.combo_bid_size;

public abstract class TransactionalBolt extends AbstractBolt implements Checkpointable {
    protected static final Logger LOG= LoggerFactory.getLogger(TransactionalBolt.class);
    public TxnManager transactionManager;
    protected int thread_Id;
    protected int tthread;
    protected int NUM_ACCESSES;
    protected int COMPUTE_COMPLEXITY;
    protected int POST_COMPUTE_COMPLEXITY;
    private int i=0;
    private int NUM_ITEMS;

//    public State state = null;
//    public OrderLock lock;//used for lock_ratio-based ordering constraint.
//    public OrderValidate orderValidate;
    public TxnContext[] txn_context = new TxnContext[combo_bid_size];
    //public SINKCombo sink=new SINKCCombo()

    public TransactionalBolt(Logger log,int fid) {
        super(log, null, null, false, 0, 1);
        this.fid=fid;
    }
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        OsUtils.configLOG(LOG);
        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 1);
        NUM_ACCESSES = 1;
        COMPUTE_COMPLEXITY = 10;
        POST_COMPUTE_COMPLEXITY = 1;
//        sink.configPrefix = this.getConfigPrefix();
//        sink.prepare(config, context, collector);
        SOURCE_CONTROL.getInstance().config(tthread);

    }
    //checkpoint
    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) {
    }
    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker,String msg) throws InterruptedException {
        this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
    }
    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker,String msg) throws InterruptedException {
        this.collector.broadcast_marker(streamId, bid, marker);//bolt needs to broadcast_marker
    }
    @Override
    public void ack_checkpoint(Marker marker) {
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }
    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }

    @Override
    public boolean checkpoint(int counter) {
        return false;
    }
    @Override
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException{};
    protected void PRE_EXECUTE(Tuple in){
        _bid = in.getBID();
        input_event = in.getValue(0);
        TxnContext temp=new TxnContext(thread_Id, this.fid, _bid);
        txn_context[0] = temp;
        sum = 0;
    }
    protected long timestamp;
    protected long _bid;
    protected Object input_event;
    int sum = 0;
    //used in the LA_CC
    protected void PostLAL_process(long bid) throws DatabaseException, InterruptedException {
    }
    //used in the LA_CC and S-Store_CC
    protected void LAL_PROCESS(long bid) throws DatabaseException, InterruptedException {
    }
    protected void POST_PROCESS(long bid, long timestamp, int i) throws InterruptedException {
    }
    //used in the T-Stream_CC
    protected void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
    }
    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        //pre stream processing phase..
        PRE_EXECUTE(in);
        PRE_TXN_PROCESS(_bid, timestamp);
    }
    //used in the S-Store_CC
    //used in the No_CC
    protected void nocc_execute(Tuple in) throws DatabaseException, InterruptedException{
        PRE_EXECUTE(in);
        TXN_PROCESS(_bid);
        POST_PROCESS(_bid,timestamp,combo_bid_size);
    }
}
