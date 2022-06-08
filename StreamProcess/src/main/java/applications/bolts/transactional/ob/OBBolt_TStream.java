package applications.bolts.transactional.ob;

import applications.events.TxnEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BidingResult;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.function.Condition;
import engine.transaction.function.DEC;
import engine.transaction.function.INC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.faulttolerance.clr.ComputationLogic;
import streamprocess.faulttolerance.clr.ComputationTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.enable_clr;
import static UserApplications.constants.OnlineBidingSystemConstants.Constant.NUM_ACCESSES_PER_BUY;

public abstract class OBBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(OBBolt_TStream.class);
    List<TxnEvent> EventsHolder=new ArrayList<>();
    public OBBolt_TStream(int fid) {
        super(LOG, fid);
        this.configPrefix="tpob";
        status=new Status();
        this.setStateful();
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext=new TxnContext(thread_Id,this.fid,in.getBID());
        TxnEvent event = (TxnEvent) in.getValue(0);
        if (event instanceof BuyingEvent) {
           Buying_request_construct((BuyingEvent) event, txnContext);
        } else if (event instanceof AlertEvent) {
            Alert_request_construct((AlertEvent) event, txnContext);
        } else {
           Topping_request_construct((ToppingEvent) event, txnContext);
        }
    }
    protected boolean Topping_request_construct(ToppingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        boolean flag=true;
        for (int i = 0; i < event.getNum_access(); i++){
            flag=transactionManager.Asy_ModifyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), new INC(event.getItemTopUp()[i]), 2);//asynchronously return.
            if(!flag){
                break;
            }
        }
        if(flag){
            EventsHolder.add(event);//mark the tuple as ``in-complete"
        }else {
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
        }
        return flag;
    }
    protected boolean Alert_request_construct(AlertEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        boolean flag=true;
        for (int i = 0; i < event.getNum_access(); i++){
            flag=transactionManager.Asy_WriteRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.getAsk_price()[i], 1);//asynchronously return.
            if(!flag){
                break;
            }
        }
        if(flag){
            EventsHolder.add(event);//mark the tuple as ``in-complete"
        }else {
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
        }
        return flag;
    }
    protected boolean Buying_request_construct(BuyingEvent event,TxnContext txnContext) throws DatabaseException, InterruptedException {
        boolean flag=true;
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; i++) {
            flag=transactionManager.Asy_ModifyRecord(//TODO: add atomicity preserving later.
                    txnContext,
                    "goods",
                    String.valueOf(event.getItemId()[i]),
                    new DEC(event.getBidQty(i)),
                    new Condition(event.getBidPrice(i), event.getBidQty(i)),
                    event.success
            );
            if(!flag){
                break;
            }
        }
        if(flag){
            EventsHolder.add(event);//mark the tuple as ``in-complete"
        }else {
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
        }
        return flag;
    }
    protected void AsyncReConstructRequest() throws InterruptedException, DatabaseException {
        Iterator<TxnEvent> it=EventsHolder.iterator();
        while (it.hasNext()){
            TxnEvent event=it.next();
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, event.getBid());
            if(event instanceof BuyingEvent){
                BuyingEvent buy_event=(BuyingEvent) event;
                for (int i = 0; i < NUM_ACCESSES_PER_BUY; i++) {
                    if(!transactionManager.Asy_ModifyRecord(//TODO: add atomicity preserving later.
                            txnContext,
                            "goods",
                            String.valueOf(buy_event.getItemId()[i]),
                            new DEC(buy_event.getBidQty(i)),
                            new Condition(buy_event.getBidPrice(i), buy_event.getBidQty(i)),
                            buy_event.success
                    )){
                        it.remove();
                        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
                        break;
                    }
                }
            }else if (event instanceof AlertEvent){
                AlertEvent alert_event=(AlertEvent) event;
                for (int i = 0; i < alert_event.getNum_access(); i++) {
                    if(!transactionManager.Asy_WriteRecord(txnContext,
                            "goods",
                            String.valueOf(alert_event.getItemId()[i]),
                            alert_event.getAsk_price()[i],
                            1)){
                        it.remove();
                        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
                        break;
                    }
                }
            }else{
                ToppingEvent toppingEvent=(ToppingEvent) event;
                for (int i=0;i<toppingEvent.getNum_access();i++){
                    if(!transactionManager.Asy_ModifyRecord(txnContext,
                            "goods",
                            String.valueOf(toppingEvent.getItemId()[i]),
                            new INC(toppingEvent.getItemTopUp()[i]),
                            2)){
                        it.remove();
                        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
                        break;
                    }
                }
            }
        }
    }
    @Override
    protected void REQUEST_REQUEST_CORE() throws InterruptedException {
        for(TxnEvent event:EventsHolder){
            if(event instanceof BuyingEvent){
                BUYING_REQUEST_CORE((BuyingEvent) event);
            }
        }
    }
    protected void BUYING_REQUEST_CORE(BuyingEvent event) {
        //measure_end if any item is not able to buy.
        event.biding_result = new BidingResult(event, event.success[0]);
    }
    @Override
    protected void REQUEST_POST() throws InterruptedException {
        for(TxnEvent event:EventsHolder){
            if(event instanceof BuyingEvent){
                BUYING_REQUEST_POST((BuyingEvent) event);
            }else if(event instanceof AlertEvent){
                ALERT_REQUEST_POST((AlertEvent) event);
            }else {
                TOPPING_REQUEST_POST((ToppingEvent) event);
            }
        }
    }
    protected void BUYING_REQUEST_POST(BuyingEvent event) throws InterruptedException {
        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true, event.getTimestamp());
    }
    protected void ALERT_REQUEST_POST(AlertEvent event) throws InterruptedException {
        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), event.alert_result, event.getTimestamp());//the tuple is finished finally.
    }

    protected void TOPPING_REQUEST_POST(ToppingEvent event) throws InterruptedException {
        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), event.topping_result, event.getTimestamp());//the tuple is finished finally.
    }
}
