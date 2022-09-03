package applications.bolts.transactional.gs;

import System.measure.MeasureTools;
import applications.events.TxnEvent;
import applications.events.gs.MicroEvent;
import applications.events.gs.MicroResult;
import engine.Exception.DatabaseException;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.transaction.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.controller.output.Determinant.InsideDeterminant;
import streamprocess.controller.output.Determinant.OutsideDeterminant;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.faulttolerance.clr.CausalService;

import java.util.*;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;
import static applications.events.DeserializeEventHelper.deserializeEvent;

public abstract class GSBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(GSBolt_TStream.class);
    private static final long serialVersionUID = -2914962488031246108L;
    List<MicroEvent> EventsHolder = new ArrayList<>();
    public GSBolt_TStream( int fid) {
        super(LOG, fid);
        this.configPrefix="tpgs";
        status = new Status();
        this.setStateful();
    }


    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id,this.fid,in.getBID());
        MicroEvent event = (MicroEvent) in.getValue(0);
        event.setTxnContext(txnContext);
        MeasureTools.Transaction_construction_begin(thread_Id, System.nanoTime());
        if (event.READ_EVENT()) {//read
            if (enable_determinants_log) {
                determinant_read_construct(event, txnContext);
            } else {
                read_construct(event, txnContext);
            }
        } else {
            if (enable_determinants_log) {
                determinant_write_construct(event, txnContext);
            } else {
                write_construct(event, txnContext);
            }
        }
        MeasureTools.Transaction_construction_acc(thread_Id, System.nanoTime());
    }
    void read_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; i++) {
            transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
        }
        EventsHolder.add(event);//mark the tuple as ``in-complete"
    }

    protected void write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
           transactionManager.Asy_WriteRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getValues()[i], event.enqueue_time);//asynchronously return.
        }
        EventsHolder.add(event);//mark the tuple as ``in-complete"
        if (enable_recovery_dependency) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            this.updateRecoveryDependency(event.getKeys(),true);
            MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
        }
    }

    void determinant_read_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEvent.contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false,true);
                    return;
                }
            }
        } else {
            read_construct(event, txnContext);
        }
    }

    protected void determinant_write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEvent.contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false,true);
                    return;
                }
            }
            for (int i = 0; i < NUM_ACCESSES; i++) {
                if (this.recoveryPartitionIds.contains(this.getPartitionId( String.valueOf(event.getKeys()[i])))) {
                    transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
                }
            }
        } else {
            write_construct(event, txnContext);
        }
    }

    protected void CommitOutsideDeterminant(long markId) throws DatabaseException, InterruptedException {
        if ((enable_key_based || this.executor.isFirst_executor()) && !this.causalService.isEmpty()) {
            for (CausalService c:this.causalService.values()) {
                for (OutsideDeterminant outsideDeterminant:c.outsideDeterminantList.get(markId)) {
                    TxnEvent event = deserializeEvent(outsideDeterminant.outSideEvent);
                    if (event.getBid() <= markId) {
                        TxnContext txnContext = new TxnContext(thread_Id,this.fid,event.getBid());
                        event.setTxnContext(txnContext);
                        MicroEvent microEvent = (MicroEvent) event;
                        if (microEvent.READ_EVENT()) {
                            determinant_read_construct(microEvent, txnContext);
                        } else {
                            determinant_write_construct(microEvent, txnContext);
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    @Override
    protected void REQUEST_CORE() throws InterruptedException {
        for (MicroEvent event:EventsHolder){
            if (event.READ_EVENT()){
                READ_CORE(event);
            }
        }
    }

    protected void REQUEST_POST() throws InterruptedException {
        if (this.markerId > recoveryId) {
            for (MicroEvent event : EventsHolder) {
                int targetId;
                if(event.READ_EVENT()){
                    targetId = READ_POST(event);
                }else {
                    targetId = WRITE_POST(event);
                }
                if (enable_upstreamBackup) {
                    MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
                    this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, new MicroResult(event.getBid(), event.getTimestamp(), false, event.sum));
                    MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
                }
            }
            MeasureTools.Upstream_backup_finish_acc(this.executor.getExecutorID());
        }
    }
    private void READ_CORE(MicroEvent event) {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];
            if (ref.isEmpty())
                return;//not yet processed.
            DataBox dataBox = ref.getRecord().getValues().get(1);
            int read_result = Integer.parseInt(dataBox.getString().trim());
            event.result[i] = read_result;
        }
    }

    protected int READ_POST(MicroEvent event) throws InterruptedException {
        for (int i = 0; i < NUM_ACCESSES;i++){
            event.sum += event.result[i];
        }
        return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, event.getTimestamp(), event.sum);//the tuple is finished finally.
    }
    protected int WRITE_POST(MicroEvent event) throws InterruptedException {
        if (enable_determinants_log) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            if (event.txnContext.isAbort.get()) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                return collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false, insideDeterminant,event.getTimestamp());
            } else {
                OutsideDeterminant outsideDeterminant = new OutsideDeterminant();
                outsideDeterminant.setOutSideEvent(event.toString());
                for (int i = 0; i < NUM_ACCESSES; i++) {
                    if (this.getPartitionId(String.valueOf(event.getKeys()[i])) != event.getPid()) {
                        outsideDeterminant.setTargetPartitionId(this.getPartitionId(String.valueOf(event.getKeys()[i])));
                    }
                }
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                if (outsideDeterminant.targetPartitionIds.size() !=0 ) {
                    return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, outsideDeterminant, event.getTimestamp());//the tuple is finished finally.
                } else {
                    return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, event.getTimestamp());//the tuple is finished finally.
                }
            }
        } else {
            if (event.txnContext.isAbort.get()) {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, event.getTimestamp());//the tuple is finished finally.
            } else {
                return collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, event.getTimestamp());//the tuple is finished finally.
            }
        }
    }
}
