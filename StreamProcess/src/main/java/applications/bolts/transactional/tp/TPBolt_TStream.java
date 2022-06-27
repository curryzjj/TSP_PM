package applications.bolts.transactional.tp;

import System.measure.MeasureTools;
import applications.events.lr.TollProcessingEvent;
import applications.events.lr.TollProcessingResult;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.function.AVG;
import engine.transaction.function.CNT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.controller.output.Determinant.InsideDeterminant;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.faulttolerance.clr.CausalService;

import java.util.ArrayDeque;
import java.util.Iterator;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class TPBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_TStream.class);
    ArrayDeque<TollProcessingEvent> LREvents = new ArrayDeque<>();
    public TPBolt_TStream(int fid) {
        super(LOG, fid);
        this.configPrefix="tptxn";
        status = new Status();
        this.setStateful();
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id, this.fid, in.getBID());
        TollProcessingEvent event = (TollProcessingEvent) in.getValue(0);
        MeasureTools.Transaction_construction_begin(this.thread_Id, System.nanoTime());
        if (enable_determinants_log) {
            Determinant_REQUEST_CONSTRUCT(event, txnContext);
        } else {
            REQUEST_CONSTRUCT(event, txnContext, false);
        }
        MeasureTools.Transaction_construction_acc(this.thread_Id, System.nanoTime());
    }
    protected boolean REQUEST_CONSTRUCT(TollProcessingEvent event, TxnContext txnContext, boolean isReConstruct) throws DatabaseException, InterruptedException {
        //some process used the transactionManager
        boolean flag = transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                ,String.valueOf(event.getSegmentId())
                ,event.getSpeed_value()[0]//holder to be filled up
                ,new AVG(event.getSpeedValue()));
        if(!flag){
            int targetId;
            if (enable_determinants_log) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, insideDeterminant, event.getTimestamp());//the tuple is finished.//the tuple is abort.
            } else {
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null , event.getTimestamp());//the tuple is finished.//the tuple is abort.
            }
            if (enable_upstreamBackup) {
                this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, new TollProcessingResult(event.getBid(), event.getTimestamp(), -1));
            }
            return false;
        }
        transactionManager.Asy_ModifyRecord_Read(txnContext
                ,"segment_cnt"
                ,String.valueOf(event.getSegmentId())
                ,event.getCount_value()[0]
                ,new CNT(event.getVid()));
        for (int i = 1; i < NUM_ACCESSES; i++) {
            transactionManager.Asy_ReadRecord(txnContext, "segment_speed", String.valueOf(event.getKeys()[i]), event.getSpeed_value()[i], null);
            transactionManager.Asy_ReadRecord(txnContext, "segment_cnt", String.valueOf(event.getKeys()[i]), event.getCount_value()[i], null);
        }
        if (!isReConstruct) {
            LREvents.add(event);
            if (enable_recovery_dependency) {
                MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
                this.updateRecoveryDependency(new int[]{event.getSegmentId()}, true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
            }
        }
        return true;
    }
    protected void Determinant_REQUEST_CONSTRUCT(TollProcessingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEvent.contains(event.getBid())){
                    return;
                }
            }
            transactionManager.Asy_ModifyRecord_Read(txnContext
                    , "segment_speed"
                    ,String.valueOf(event.getSegmentId())
                    ,event.getSpeed_value()[0]//holder to be filled up
                    ,new AVG(event.getSpeedValue()));
            transactionManager.Asy_ModifyRecord_Read(txnContext
                    ,"segment_cnt"
                    ,String.valueOf(event.getSegmentId())
                    ,event.getSpeed_value()[0]
                    ,new CNT(event.getVid()));
        } else {
            REQUEST_CONSTRUCT(event, txnContext, false);
        }
    }
    protected void AsyncReConstructRequest() throws DatabaseException, InterruptedException {
        for (Iterator<TollProcessingEvent> it = LREvents.iterator(); it.hasNext(); ) {
            TollProcessingEvent event = it.next();
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, event.getBid());
            boolean flag = REQUEST_CONSTRUCT(event, txnContext, true);
            if(!flag){
                it.remove();
            }
        }
    }
    protected void REQUEST_CORE() {
        for (TollProcessingEvent event : LREvents) {
            TS_REQUEST_CORE(event);
        }
    }
    private void TS_REQUEST_CORE(TollProcessingEvent event) {
       for (int i = 0; i < NUM_ACCESSES; i++) {
           event.spendValues[i] = event.getSpeed_value()[i].getRecord().getValue().getDouble();
           event.cntValues[i] = event.getCount_value()[i].getRecord().getValue().getInt();
       }
    }
    protected void REQUEST_POST() throws InterruptedException {
        if (this.markerId > recoveryId) {
            for (TollProcessingEvent event : LREvents) {
                int targetId = TP_REQUEST_POST(event);
                if (enable_upstreamBackup) {
                    MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
                    this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, new TollProcessingResult(event.getBid(), event.getTimestamp(),event.toll));
                    MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
                }
            }
            MeasureTools.Upstream_backup_finish_acc(this.executor.getExecutorID());
        }
    }
    int TP_REQUEST_POST(TollProcessingEvent event) throws InterruptedException {
        //Nothing to determinant log
        double spendValue = 0;
        int cntValue = 0;
        for (int i =0; i < NUM_ACCESSES; i++) {
            spendValue = spendValue + event.spendValues[i];
            cntValue = cntValue + event.cntValues[i];
        }
        //Some UDF function
        event.toll = spendValue / cntValue;
        return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, event.getTimestamp(), event.toll);//the tuple is finished.
    }

    protected void CommitOutsideDeterminant(long markerId) {
        //No outsideDeterminant
    }
}
