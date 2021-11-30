package streamprocess.components.operators.api;

import System.util.OsUtils;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.TxnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.faulttolerance.checkpoint.emitMarker;
import utils.SOURCE_CONTROL;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.combo_bid_size;

public abstract class TransactionalBolt extends AbstractBolt implements emitMarker {
    protected static final Logger LOG= LoggerFactory.getLogger(TransactionalBolt.class);
    public TxnManager transactionManager;
    protected int thread_Id;
    protected int tthread;
    protected int NUM_ACCESSES;
    protected int COMPUTE_COMPLEXITY;
    protected int POST_COMPUTE_COMPLEXITY;
    private int i=0;
    private int NUM_ITEMS;
    public List<Tuple> bufferedTuple=new ArrayList<>();
    public TxnContext[] txn_context = new TxnContext[combo_bid_size];

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
    @Override
    public void forward_marker(int sourceId, long bid, Marker marker, String msg) throws InterruptedException {
        this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
    }
    @Override
    public void forward_marker(int sourceTask, String streamId, long bid, Marker marker, String msg) throws InterruptedException {
        this.collector.broadcast_marker(streamId, bid, marker);//bolt needs to broadcast_marker
    }
    @Override
    public void ack_marker(Marker marker) {
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }
    @Override
    public void earlier_ack_marker(Marker marker) {

    }
    @Override
    public boolean marker() {
        return false;
    }
    @Override
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException;
    protected void PRE_EXECUTE(Tuple in){
        bufferedTuple.add(in);
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
    //used in the T-Stream_CC
    protected void PRE_TXN_PROCESS(Tuple input_event) throws DatabaseException, InterruptedException {
    }
}
