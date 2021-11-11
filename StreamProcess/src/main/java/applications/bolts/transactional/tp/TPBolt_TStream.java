package applications.bolts.transactional.tp;

import applications.DataTypes.PositionReport;
import applications.events.lr.LREvent;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.function.AVG;
import engine.transaction.function.CNT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.checkpoint.Status;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.BrokenBarrierException;

import static UserApplications.CONTROL.combo_bid_size;

public class TPBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_TStream.class);
    ArrayDeque<LREvent> LREvents = new ArrayDeque<>();
    public TPBolt_TStream(int fid) {
        super(LOG,fid);
        this.configPrefix="tptxn";
        status=new Status();
    }
    @Override
    protected void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            LREvent event = new LREvent((PositionReport) input_event,tthread,bid);
            (event).setTimestamp(timestamp);
            REQUEST_CONSTRUCT(event, txnContext);
        }
    }
    protected void REQUEST_CONSTRUCT(LREvent event, TxnContext txnContext) throws DatabaseException {
        //some process used the transactionManager
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                ,String.valueOf(event.getPOSReport().getSegment())
                ,event.speed_value//holder to be filled up
                ,new AVG(event.getPOSReport().getSpeed())
        );
        transactionManager.Asy_ModifyRecord_Read(txnContext
                ,"segment_cnt"
                ,String.valueOf(event.getPOSReport().getSegment())
                ,event.count_value
                ,new CNT(event.getPOSReport().getVid()));
        LREvents.add(event);
    }
    @Override
    protected void TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException {
        transactionManager.start_evaluate(thread_Id,this.fid);
        REQUEST_REQUEST_CORE();
        REQUEST_POST();
        LREvents.clear();//clear stored events.
    }
    protected void REQUEST_REQUEST_CORE() {
        for (LREvent event : LREvents) {
            TS_REQUEST_CORE(event);
        }
    }
    private void TS_REQUEST_CORE(LREvent event) {
        //get the value from the event
        event.count=event.count_value.getRecord().getValue().getInt();
        System.out.println(event.count);
        event.lav=event.speed_value.getRecord().getValue().getDouble();
        System.out.println(event.lav);
    }
    protected void REQUEST_POST() throws InterruptedException {
        for (LREvent event : LREvents) {
            TP_REQUEST_POST(event);
        }
    }
    void TP_REQUEST_POST(LREvent event) throws InterruptedException {
        //TODO:some process to Post the event to the sink or emit
        collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
    }

}
