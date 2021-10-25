package applications.bolts.transactional.tp;

import applications.events.lr.LREvent;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.function.AVG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;

import java.util.ArrayDeque;

import static UserApplications.CONTROL.combo_bid_size;

public class TPBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_TStream.class);
    ArrayDeque<LREvent> LREvents = new ArrayDeque<>();
    public TPBolt_TStream(Logger log, int fid) {
        super(log,fid);
        this.configPrefix="tptxn";
        //state=new ValueState();
    }
    protected void REQUEST_CONSTRUCT(LREvent event, TxnContext txnContext) throws DatabaseException {
        //some process used the transactionManager
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                ,String.valueOf(event.getPOSReport().getSegment())
                ,event.speed_value//holder to be filled up
                ,new AVG(event.getPOSReport().getSpeed())
        );
    }
    protected void REQUEST_REQUEST_CORE() {
        for (LREvent event : LREvents) {
            TS_REQUEST_CORE(event);
        }
    }
    private void TS_REQUEST_CORE(LREvent event) {
        //get the value from the event
    }
    protected void REQUEST_POST() throws InterruptedException {
        for (LREvent event : LREvents) {
            TP_REQUEST_POST(event);
        }
    }
    void TP_REQUEST_POST(LREvent event) throws InterruptedException {
        //some process to Post the event to the sink or emit
    }
    @Override
    protected void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            LREvent event = (LREvent) input_event;
            (event).setTimestamp(timestamp);
            REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    @Override
    protected void TXN_PROCESS() throws DatabaseException, InterruptedException {
        //transaction.start_evaluate
        REQUEST_REQUEST_CORE();
        REQUEST_POST();
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {//not used

    }

}
