package applications.bolts.transactional.tp;

import applications.DataTypes.PositionReport;
import applications.events.lr.LREvent;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.function.AVG;
import engine.transaction.function.CNT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;

import java.util.ArrayDeque;
import java.util.Iterator;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;

public abstract class TPBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_TStream.class);
    ArrayDeque<LREvent> LREvents = new ArrayDeque<>();
    public TPBolt_TStream(int fid) {
        super(LOG,fid);
        this.configPrefix="tptxn";
        status=new Status();
        this.setStateful();
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, in.getBID());
            LREvent event = new LREvent((PositionReport) in.getValue(0),tthread,in.getBID());
            REQUEST_CONSTRUCT(event, txnContext);
    }
    protected void REQUEST_CONSTRUCT(LREvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //some process used the transactionManager
        boolean flag=transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                ,String.valueOf(event.getPOSReport().getSegment())
                ,event.speed_value//holder to be filled up
                ,new AVG(event.getPOSReport().getSpeed())
        );
        if(!flag){
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            return;
        }
       transactionManager.Asy_ModifyRecord_Read(txnContext
                ,"segment_cnt"
                ,String.valueOf(event.getPOSReport().getSegment())
                ,event.count_value
                ,new CNT(event.getPOSReport().getVid()));
        LREvents.add(event);
    }
    protected void AsyncReConstructRequest() throws DatabaseException, InterruptedException {
        for (Iterator<LREvent> it = LREvents.iterator(); it.hasNext(); ) {
            LREvent event = it.next();
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, event.getBid());
            boolean flag=transactionManager.Asy_ModifyRecord_Read(txnContext
                    , "segment_speed"
                    ,String.valueOf(event.getPOSReport().getSegment())
                    ,event.speed_value//holder to be filled up
                    ,new AVG(event.getPOSReport().getSpeed())
            );
            if(!flag){
                it.remove();
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
                continue;
            }
            boolean flag1=transactionManager.Asy_ModifyRecord_Read(txnContext
                    ,"segment_cnt"
                    ,String.valueOf(event.getPOSReport().getSegment())
                    ,event.count_value
                    ,new CNT(event.getPOSReport().getVid()));
        }
    }
    protected void REQUEST_REQUEST_CORE() {
        for (LREvent event : LREvents) {
            TS_REQUEST_CORE(event);
        }
    }
    private void TS_REQUEST_CORE(LREvent event) {
        event.count=event.count_value.getRecord().getValue().getInt();
        event.lav=event.speed_value.getRecord().getValue().getDouble();
    }
    protected void REQUEST_POST() throws InterruptedException {
        for (LREvent event : LREvents) {
            TP_REQUEST_POST(event);
        }
    }
    void TP_REQUEST_POST(LREvent event) throws InterruptedException {
        //TODO:some process to Post the event to the sink or emit
        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true,event.getTimestamp());//the tuple is finished.
    }

}
