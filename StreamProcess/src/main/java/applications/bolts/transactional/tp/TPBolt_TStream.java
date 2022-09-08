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

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class TPBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_TStream.class);
    private static final long serialVersionUID = -4887076370033768805L;
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
            REQUEST_CONSTRUCT(event, txnContext);
        }
        MeasureTools.Transaction_construction_acc(this.thread_Id, System.nanoTime());
    }
    protected void REQUEST_CONSTRUCT(TollProcessingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                ,String.valueOf(event.getSegmentId())
                ,event.getSpeed_value()[0]//holder to be filled up
                ,new AVG(event.getSpeedValue()));
        transactionManager.Asy_ModifyRecord_Read(txnContext
                ,"segment_cnt"
                ,String.valueOf(event.getSegmentId())
                ,event.getCount_value()[0]
                ,new CNT(event.getVid()));
        LREvents.add(event);
        if (enable_recovery_dependency) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            this.updateRecoveryDependency(new int[]{event.getSegmentId()}, true);
            MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
        }
    }
    protected void Determinant_REQUEST_CONSTRUCT(TollProcessingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEventList.get(event.getBid()).contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false, true);
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
            REQUEST_CONSTRUCT(event, txnContext);
        }
    }
    protected void REQUEST_CORE() {
        if (this.markerId > recoveryId) {
            for (TollProcessingEvent event : LREvents) {
                TS_REQUEST_CORE(event);
            }
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
        if (enable_determinants_log) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            if (event.txnContext.isAbort.get()) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, insideDeterminant,null, event.getTimestamp());//the tuple is finished.
            } else {
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                double spendValue = 0;
                int cntValue = 0;
                for (int i = 0; i < NUM_ACCESSES; i++) {
                    spendValue = spendValue + event.spendValues[i];
                    cntValue = cntValue + event.cntValues[i];
                }
                //Some UDF function
                event.toll = spendValue / cntValue;
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, event.getTimestamp(), event.toll);//the tuple is finished.
            }
        } else {
            if (event.txnContext.isAbort.get()) {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp());//the tuple is finished.
            } else {
                double spendValue = 0;
                int cntValue = 0;
                for (int i = 0; i < NUM_ACCESSES; i++) {
                    spendValue = spendValue + event.spendValues[i];
                    cntValue = cntValue + event.cntValues[i];
                }
                //Some UDF function
                event.toll = spendValue / cntValue;
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null,null, event.getTimestamp());//the tuple is finished.
            }
        }
    }

    protected void CommitOutsideDeterminant(long markerId) {
        //No outsideDeterminant
    }
}
