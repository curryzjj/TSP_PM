package applications.bolts.transactional.gs;

import applications.events.MicroEvent;
import engine.Exception.DatabaseException;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.transaction.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.NUM_ACCESSES;

public abstract class GSBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(GSBolt_TStream.class);
    Collection<MicroEvent> EventsHolder=new ArrayDeque<>();
    public GSBolt_TStream( int fid) {
        super(LOG, fid);
        this.configPrefix="tpgs";
        status=new Status();
        this.setStateful();
    }


    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext=new TxnContext(thread_Id,this.fid,in.getBID());
        MicroEvent event=(MicroEvent) in.getValue(0);
        if (event.READ_EVENT()) {//read
            read_construct(event, txnContext);
        } else {
            write_construct(event, txnContext);
        }
    }
    public void BUFFER_PROCESS() throws DatabaseException, InterruptedException {
        if(bufferedTuple.isEmpty()){
            return;
        }else{
            Iterator<Tuple> bufferedTuples=bufferedTuple.iterator();
            while (bufferedTuples.hasNext()){
                PRE_TXN_PROCESS(bufferedTuples.next());
            }
        }
    }
    boolean read_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        boolean flag=true;
        for (int i = 0; i < NUM_ACCESSES; i++) {
            //it simply constructs the operations and return.
            flag=transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
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

    protected boolean write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
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
        }else {
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
        }
        return flag;
    }
    protected void AsyncReConstructRequest() throws DatabaseException, InterruptedException {
        Iterator<MicroEvent> it=EventsHolder.iterator();
        if (it.hasNext()){
            MicroEvent event=it.next();
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, event.getBid());
            if(event.READ_EVENT()){
                if(!read_construct(event,txnContext)){
                    it.remove();
                }
            }else{
                if(!write_construct(event,txnContext)){
                    it.remove();
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
        for (MicroEvent event : EventsHolder) {
            if(event.READ_EVENT()){
                READ_POST(event);
            }else {
                WRITE_POST(event);
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
        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true, event.getTimestamp(),sum);//the tuple is finished finally.
    }
    protected void WRITE_POST(MicroEvent event) throws InterruptedException {
        collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), true, event.getTimestamp());//the tuple is finished finally.
    }
}
