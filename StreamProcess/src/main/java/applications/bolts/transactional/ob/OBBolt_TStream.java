package applications.bolts.transactional.ob;

import System.measure.MeasureTools;
import System.sink.helper.ApplicationResult;
import applications.events.TxnEvent;
import applications.events.ob.*;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.function.Condition;
import engine.transaction.function.DEC;
import engine.transaction.function.INC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.controller.output.Determinant.InsideDeterminant;
import streamprocess.controller.output.Determinant.OutsideDeterminant;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.faulttolerance.clr.CausalService;

import java.util.ArrayList;
import java.util.List;


import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;
import static applications.events.DeserializeEventHelper.deserializeEvent;


public abstract class OBBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(OBBolt_TStream.class);
    private static final long serialVersionUID = 6572082902742007113L;
    List<TxnEvent> EventsHolder = new ArrayList<>();
    public OBBolt_TStream(int fid) {
        super(LOG, fid);
        this.configPrefix="tpob";
        status = new Status();
        this.setStateful();
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id,this.fid,in.getBID());
        TxnEvent event = (TxnEvent) in.getValue(0);
        event.setTxnContext(txnContext);
        MeasureTools.Transaction_construction_begin(this.thread_Id, System.nanoTime());
        if (event instanceof BuyingEvent) {
            if (enable_determinants_log) {
                Determinant_Buying_request_construct((BuyingEvent) event, txnContext);
            } else {
                Buying_request_construct((BuyingEvent) event, txnContext);
            }
        } else if (event instanceof AlertEvent) {
            if (enable_determinants_log) {
                Determinant_Alert_request_construct((AlertEvent) event, txnContext);
            } else {
                Alert_request_construct((AlertEvent) event, txnContext);
            }
        } else {
            if (enable_determinants_log) {
                Determinant_Topping_request_construct((ToppingEvent) event, txnContext);
            } else {
                Topping_request_construct((ToppingEvent) event, txnContext);
            }
        }
        MeasureTools.Transaction_construction_acc(this.thread_Id, System.nanoTime());
    }
    protected void Topping_request_construct(ToppingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < event.getNum_access(); i++){
            transactionManager.Asy_ModifyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), new INC(event.getItemTopUp()[i]), 2);//asynchronously return.
        }
        EventsHolder.add(event);//mark the tuple as ``in-complete"
        if (enable_recovery_dependency) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            this.updateRecoveryDependency(event.getItemId(), true);
            MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
        }
    }
    protected void Alert_request_construct(AlertEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < event.getNum_access(); i++){
            transactionManager.Asy_WriteRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.getAsk_price()[i], 1);//asynchronously return.
        }
        EventsHolder.add(event);//mark the tuple as ``in-complete"
        if (enable_recovery_dependency) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            this.updateRecoveryDependency(event.getItemId(), true);
            MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
        }
    }
    protected void Buying_request_construct(BuyingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; i++) {
           transactionManager.Asy_ModifyRecord(//TODO: add atomicity preserving later.
                    txnContext,
                    "goods",
                    String.valueOf(event.getItemId()[i]),
                    new DEC(event.getBidQty(i)),
                    new Condition(event.getBidPrice(i), event.getBidQty(i)),
                    event.success
            );
        }
        EventsHolder.add(event);//mark the tuple as ``in-complete"
        if (enable_recovery_dependency) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            this.updateRecoveryDependency(event.getItemId(), true);
            MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
        }
    }
    protected void CommitOutsideDeterminant(long markerId) throws DatabaseException, InterruptedException {
        if ((enable_key_based || this.executor.isFirst_executor()) && !this.causalService.isEmpty()) {
            for (CausalService c:this.causalService.values()) {
                if (c.outsideDeterminantList.get(markerId) != null) {
                    List<Long> isCommit = new ArrayList<>();
                    for (OutsideDeterminant outsideDeterminant:c.outsideDeterminantList.get(markerId)) {
                        TxnEvent event = deserializeEvent(outsideDeterminant.outSideEvent);
                        if (!isCommit.contains(event.getBid())) {
                            TxnContext txnContext = new TxnContext(thread_Id,this.fid,event.getBid());
                            event.setTxnContext(txnContext);
                            if (event instanceof BuyingEvent) {
                                Determinant_Buying_request_construct((BuyingEvent) event, txnContext);
                            } else if (event instanceof AlertEvent){
                                Determinant_Alert_request_construct((AlertEvent) event, txnContext);
                            } else {
                                Determinant_Topping_request_construct((ToppingEvent) event, txnContext);
                            }
                            isCommit.add(event.getBid());
                        }
                    }
                }
            }
        }
    }

    protected void Determinant_Topping_request_construct(ToppingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.getAbortEventsByMarkerId(event.getBid()).contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false, true);
                    return;
                }
            }
            for (int i = 0; i < NUM_ACCESSES; i++){
                if (this.recoveryPartitionIds.contains(this.getPartitionId(String.valueOf(event.getItemId()[i])))) {
                    transactionManager.Asy_ModifyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), new INC(event.getItemTopUp()[i]), 2);//asynchronously return.
                }
            }
        } else {
            Topping_request_construct(event, txnContext);
        }
    }
    protected void Determinant_Alert_request_construct(AlertEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.getAbortEventsByMarkerId(event.getBid()).contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false, true);
                    return;
                }
            }
            for (int i = 0; i < NUM_ACCESSES; i++){
                if (this.recoveryPartitionIds.contains(this.getPartitionId(String.valueOf(event.getItemId()[i])))) {
                    transactionManager.Asy_WriteRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.getAsk_price()[i], 1);//asynchronously return.
                }
            }
        } else {
            Alert_request_construct(event, txnContext);
        }
    }
    protected void Determinant_Buying_request_construct(BuyingEvent event,TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.getAbortEventsByMarkerId(event.getBid()).contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false, true);
                    return;
                }
            }
            for (int i = 0; i < NUM_ACCESSES; i++) {
                if (this.recoveryPartitionIds.contains(this.getPartitionId(String.valueOf(event.getItemId()[i])))) {
                    transactionManager.Asy_ModifyRecord(
                            txnContext,
                            "goods",
                            String.valueOf(event.getItemId()[i]),
                            new DEC(event.getBidQty(i)),
                            new Condition(event.getBidPrice(i), event.getBidQty(i)),
                            event.success
                    );
                }
            }
        } else {
            Buying_request_construct(event, txnContext);
        }
    }
    @Override
    protected void REQUEST_CORE() throws InterruptedException {
        for(TxnEvent event:EventsHolder){
            if(event instanceof BuyingEvent){
                BUYING_REQUEST_CORE((BuyingEvent) event);
            }
        }
    }
    protected void BUYING_REQUEST_CORE(BuyingEvent event) {
        //measure_end if any item is not able to buy.
        event.biding_result = event.success[0];
    }
    @Override
    protected void REQUEST_POST() throws InterruptedException {
        //deduplication
        if (this.markerId > recoveryId) {
            for(TxnEvent event:EventsHolder){
                int targetId;
                if(event instanceof BuyingEvent){
                    targetId = BUYING_REQUEST_POST((BuyingEvent) event);
                }else if(event instanceof AlertEvent){
                    targetId = ALERT_REQUEST_POST((AlertEvent) event);
                }else {
                    targetId = TOPPING_REQUEST_POST((ToppingEvent) event);
                }
                if (enable_upstreamBackup) {
                    MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
                    this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, new OnlineBidingResult(event.getBid(), 0));
                    MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
                }
            }
            MeasureTools.Upstream_backup_finish_acc(this.executor.getExecutorID());
        }
    }
    protected int BUYING_REQUEST_POST(BuyingEvent event) throws InterruptedException {
        double result;
        if (event.success[0]) {
            result = 1;
        } else {
            result = 0;
        }
        if (enable_determinants_log) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            if (event.txnContext.isAbort.get()) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                return collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false, insideDeterminant, null,event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{-1.0}));
            } else {
                OutsideDeterminant outsideDeterminant = new OutsideDeterminant();
                for (int itemId : event.getItemId()) {
                    if (this.getPartitionId(String.valueOf(itemId)) != event.getPid()) {
                        outsideDeterminant.setTargetPartitionId(this.getPartitionId(String.valueOf(itemId)));
                    }
                }
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                if (!outsideDeterminant.targetPartitionIds.isEmpty()) {
                    outsideDeterminant.setOutSideEvent(event.toString());
                    return collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true,null, outsideDeterminant, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{result}));//the tuple is finished.
                } else {
                    return collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{result}));//the tuple is finished.
                }
            }
        } else {
            if (event.txnContext.isAbort.get()) {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished.
            } else {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null,null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{result}));//the tuple is finished.
            }
        }
    }
    protected int ALERT_REQUEST_POST(AlertEvent event) throws InterruptedException {
        if (enable_determinants_log) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            if (event.txnContext.isAbort.get()) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                return collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false, insideDeterminant,null,new ApplicationResult(event.getBid(), new Double[]{-1.0}));
            } else {
                OutsideDeterminant outsideDeterminant = new OutsideDeterminant();
                for (int itemId : event.getItemId()) {
                    if (this.getPartitionId(String.valueOf(itemId)) != event.getPid()) {
                        outsideDeterminant.setTargetPartitionId(this.getPartitionId(String.valueOf(itemId)));
                    }
                }
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                if (!outsideDeterminant.targetPartitionIds.isEmpty()) {
                    outsideDeterminant.setOutSideEvent(event.toString());
                    return collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true, null, outsideDeterminant, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished.
                } else {
                    return collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true, null,null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished.
                }
            }
        } else {
            if (event.txnContext.isAbort.get()) {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished finally.
            } else {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished finally.
            }
        }
    }

    protected int TOPPING_REQUEST_POST(ToppingEvent event) throws InterruptedException {
        if (enable_determinants_log) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            if (event.txnContext.isAbort.get()) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, insideDeterminant, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{-1.0}));
            } else {
                OutsideDeterminant outsideDeterminant = new OutsideDeterminant();
                for (int itemId : event.getItemId()) {
                    if (this.getPartitionId(String.valueOf(itemId)) != event.getPid()) {
                        outsideDeterminant.setTargetPartitionId(this.getPartitionId(String.valueOf(itemId)));
                    }
                }
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                if (!outsideDeterminant.targetPartitionIds.isEmpty()) {
                    outsideDeterminant.setOutSideEvent(event.toString());
                    return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, outsideDeterminant, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished.
                } else {
                    return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished.
                }
            }
        } else {
            if (event.txnContext.isAbort.get()) {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null,null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished finally.
            } else {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished finally.
            }
        }
    }
}
