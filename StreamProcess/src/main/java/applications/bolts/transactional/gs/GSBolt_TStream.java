package applications.bolts.transactional.gs;

import applications.events.TxnEvent;
import applications.events.gs.MicroEvent;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class GSBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(GSBolt_TStream.class);
    private static final long serialVersionUID = -2914962488031246108L;
    List<MicroEvent> EventsHolder = new ArrayList<>();
    public GSBolt_TStream( int fid) {
        super(LOG, fid);
        this.configPrefix="tpgs";
        status=new Status();
        this.setStateful();
    }


    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext=new TxnContext(thread_Id,this.fid,in.getBID());
        MicroEvent event = (MicroEvent) in.getValue(0);
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
    }
    void read_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        boolean flag=true;
        for (int i = 0; i < NUM_ACCESSES; i++) {
            flag = transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
            if(!flag){
                break;
            }
        }
        if(flag){
            EventsHolder.add(event);//mark the tuple as ``in-complete"
            if (enable_recovery_dependency) {
                this.updateRecoveryDependency(event.getKeys(),false);
            }
        }else {
            if (enable_determinants_log) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid());
                insideDeterminant.setAbort(true);
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,insideDeterminant,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            } else {
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,null,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            }
        }
    }

    protected void write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        boolean flag=true;
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
            flag=transactionManager.Asy_WriteRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getValues()[i], event.enqueue_time);//asynchronously return.
            if(!flag){
                break;
            }
        }
        if(flag){
            EventsHolder.add(event);//mark the tuple as ``in-complete"
            if (enable_recovery_dependency) {
                this.updateRecoveryDependency(event.getKeys(),true);
            }
        }else {
            if (enable_determinants_log) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid());
                insideDeterminant.setAbort(true);
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,insideDeterminant,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            } else {
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,null,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            }
        }
    }
    protected void AsyncReConstructRequest() throws DatabaseException, InterruptedException {
        Iterator<MicroEvent> it=EventsHolder.iterator();
        while (it.hasNext()){
            MicroEvent event=it.next();
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, event.getBid());
            if(event.READ_EVENT()){
                for (int i = 0; i < NUM_ACCESSES; ++i) {
                    //it simply construct the operations and return.
                    if(!transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time)){
                        it.remove();
                        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
                        break;
                    }//asynchronously return.
                }
            }else{
                for (int i = 0; i < NUM_ACCESSES; ++i) {
                    //it simply construct the operations and return.
                    if(!transactionManager.Asy_WriteRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getValues()[i], event.enqueue_time)){
                        it.remove();
                        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
                        break;
                    }//asynchronously return.
                }
            }
        }
    }
    void determinant_read_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEvent.contains(event.getBid())){
                    return;
                }
            }
            for (int i = 0; i < NUM_ACCESSES; i++) {
                if(this.executor.operator.getExecutorIDList().get(this.getPartitionId(String.valueOf(event.getKeys()[i]))) != this.executor.getExecutorID()) {
                    for (CausalService c:this.causalService.values()) {
                        if (c.insideDeterminant.contains(event.getBid())){
                            event.getRecord_refs()[i].setRecord(c.insideDeterminant.get(event.getBid()).ackValues.get(String.valueOf(event.getKeys()[i])));
                            break;
                        }
                    }
                } else {
                    transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
                }
            }
        } else {
            read_construct(event,txnContext);
        }
    }

    protected void determinant_write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEvent.contains(event.getBid())){
                    return;
                }
            }
            for (int i = 0; i < NUM_ACCESSES; i++) {
                if (this.executor.operator.getExecutorIDList().get(this.getPartitionId(String.valueOf(event.getKeys()[i]))) == this.executor.getExecutorID()) {
                    transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
                }
            }
        } else {
            write_construct(event,txnContext);
        }
    }
    protected void CommitOutsideDeterminant(long markId) throws DatabaseException, InterruptedException {
        for (CausalService c:this.causalService.values()) {
            for (OutsideDeterminant outsideDeterminant:c.outsideDeterminant) {
                if (outsideDeterminant.outSideEvent.getBid() < markId) {
                    TxnContext txnContext=new TxnContext(thread_Id,this.fid,outsideDeterminant.outSideEvent.getBid());
                    MicroEvent event = (MicroEvent) outsideDeterminant.outSideEvent;
                    if (event.READ_EVENT()) {
                        determinant_read_construct(event, txnContext);
                    } else {
                        determinant_write_construct(event, txnContext);
                    }
                } else {
                    break;
                }
            }
        }
    }
    protected void REQUEST_CORE(){
        for (MicroEvent event:EventsHolder){
            if (event.READ_EVENT()){
                READ_CORE(event);
            }
        }
    }
    protected void REQUEST_POST() throws InterruptedException {
        if (this.markerId > recoveryId) {
            for (MicroEvent event : EventsHolder) {
                if(event.READ_EVENT()){
                    READ_POST(event);
                }else {
                    WRITE_POST(event);
                }
            }
        }
    }
    private boolean READ_CORE(MicroEvent event) {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];
            if (ref.isEmpty())
                return false;//not yet processed.
            DataBox dataBox = ref.getRecord().getValues().get(1);
            int read_result = Integer.parseInt(dataBox.getString().trim());
            event.result[i] = read_result;
        }
        return true;
    }

    protected void READ_POST(MicroEvent event) throws InterruptedException {
        int sum=0;
        for (int i=0;i<NUM_ACCESSES;i++){
            sum+=event.result[i];
        }
        if (enable_determinants_log) {
            InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid());
            for (int i = 0;i<NUM_ACCESSES;i++){
                if (this.executor.operator.getExecutorIDList().get(this.getPartitionId(String.valueOf(event.getKeys()[i]))) != this.executor.getExecutorID()) {
                    insideDeterminant.setAckValues(String.valueOf(event.getKeys()[i]),event.getRecord_refs()[i].getRecord());
                }
            }
            if (insideDeterminant.ackValues.size() !=0) {
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true,insideDeterminant, event.getTimestamp(),sum);//the tuple is finished finally.
            } else {
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true,null, event.getTimestamp(),sum);//the tuple is finished finally.
            }
        } else {
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true, null, event.getTimestamp(),sum);//the tuple is finished finally.
        }
    }
    protected void WRITE_POST(MicroEvent event) throws InterruptedException {
        if (enable_determinants_log) {
            OutsideDeterminant outsideDeterminant = new OutsideDeterminant();
            outsideDeterminant.setOutSideEvent(event);
            //TODO: just add non-determinant event
            for (int i = 0; i < NUM_ACCESSES; i++) {
                if (this.executor.operator.getExecutorIDList().get(this.getPartitionId(String.valueOf(event.getKeys()[i]))) != this.executor.getExecutorID()) {
                    outsideDeterminant.setTargetId(this.getPartitionId(String.valueOf(event.getKeys()[i])) + this.executor.operator.getExecutorIDList().get(0));
                }
            }
            if (outsideDeterminant.targetIds.size() !=0 ) {
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true,outsideDeterminant, event.getTimestamp());//the tuple is finished finally.
            } else {
                collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true,null,event.getTimestamp());//the tuple is finished finally.
            }
        } else {
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true,null,event.getTimestamp());//the tuple is finished finally.
        }
    }
}
