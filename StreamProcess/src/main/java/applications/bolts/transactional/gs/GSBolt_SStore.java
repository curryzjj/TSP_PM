package applications.bolts.transactional.gs;

import System.sink.helper.ApplicationResult;
import UserApplications.SOURCE_CONTROL;
import applications.events.GlobalSorter;
import applications.events.TxnEvent;
import applications.events.gs.MicroEvent;
import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.table.datatype.DataBox;
import engine.table.tableRecords.SchemaRecordRef;
import engine.transaction.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltSStore;
import streamprocess.faulttolerance.checkpoint.Status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.NUM_ACCESSES;
import static UserApplications.CONTROL.PARTITION_NUM;
import static UserApplications.constants.GrepSumConstants.Constant.VALUE_LEN;
import static engine.Meta.MetaTypes.AccessType.READ_ONLY;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;

public abstract class GSBolt_SStore extends TransactionalBoltSStore {
    private static final long serialVersionUID = 7281096614725470168L;
    private static final Logger LOG= LoggerFactory.getLogger(GSBolt_SStore.class);
    public GSBolt_SStore(int fid) {
        super(LOG, fid);
        this.configPrefix="gstxn";
        status = new Status();
        this.setStateful();
    }

    @Override
    public boolean Sort_Lock(int thread_Id) {
        if (SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id)) {
            LA_RESETALL(transactionManager, thread_Id);
            //Sort
            if (thread_Id == 0) {
                int[] p_bids = new int[(int) PARTITION_NUM];
                HashMap<Integer, Integer> pids = new HashMap<>();//<partitionId, batchId>
                for (TxnEvent event : GlobalSorter.sortedEvents) {
                    if (event instanceof MicroEvent) {
                        parseMicroEvent((MicroEvent) event, pids);
                        event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                        pids.replaceAll((k, v) -> p_bids[k]++);
                    } else {
                        throw new UnsupportedOperationException();
                    }
                    pids.clear();
                }
                GlobalSorter.sortedEvents.clear();
            }
        } else {
            return false;
        }
        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
        return true;
    }
    private void parseMicroEvent(MicroEvent event, HashMap<Integer, Integer> pids) {
        for (int key : event.getKeys()) {
            pids.put(key / partition_delta, 0);
        }
    }

    @Override
    public void LAL(TxnEvent event) throws DatabaseException {
        MicroEvent microEvent = (MicroEvent) event;
        boolean flag = ((MicroEvent) event).READ_EVENT();
        if (flag) {
            READ_LOCK_AHEAD(microEvent);
        } else {
            WRITE_LOCK_AHEAD(microEvent);
        }
    }
    protected void READ_LOCK_AHEAD(MicroEvent event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            transactionManager.lock_ahead("MicroTable", String.valueOf(event.getKeys()[i]), READ_ONLY);
        }
    }
    protected void WRITE_LOCK_AHEAD(MicroEvent event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            transactionManager.lock_ahead("MicroTable", String.valueOf(event.getKeys()[i]), READ_WRITE);
        }
    }

    @Override
    public void PostLAL_Process(TxnEvent event) throws DatabaseException {
        MicroEvent microEvent = (MicroEvent) event;
        boolean flag = microEvent.READ_EVENT();
        TxnContext txnContext = new TxnContext(this.thread_Id, fid, microEvent.getBid());
        if (!checkAbort(microEvent)) {
            if (flag) {
                process_request_noLock(microEvent,txnContext,READ_ONLY);
                for (int i = 0; i < NUM_ACCESSES; i ++) {
                    SchemaRecordRef ref = microEvent.getRecord_refs()[i];
                    if (ref.isEmpty()) {
                        return;//not yet processed.
                    }
                    DataBox dataBox = ref.getRecord().getValues().get(1);
                    int read_result = Integer.parseInt(dataBox.getString().trim());
                    microEvent.result[i] = read_result;
                    microEvent.sum += microEvent.result[i];
                }
            } else {
                process_request_noLock(microEvent,txnContext,READ_WRITE);
                for (int i = 0; i < NUM_ACCESSES; i++) {
                    SchemaRecordRef ref = microEvent.getRecord_refs()[i];
                    if (ref.isEmpty()) {
                        return;//not yet processed.
                    }
                    List<DataBox> values = ref.getRecord().getValues();
                    int read_result = Integer.parseInt(values.get(1).getString().trim());
                    ref.getRecord().getValues().get(1).setString(String.valueOf(read_result + microEvent.getValues()[i]), VALUE_LEN);
                }
            }
        }
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < NUM_ACCESSES; ++i) {
           keys.add("MicroTable_" + getPartitionId(String.valueOf(microEvent.getKeys()[i])));
        }
        transactionManager.CommitTransaction(keys);
    }
    private void process_request_noLock(MicroEvent microEvent, TxnContext txnContext, MetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "MicroTable",
                    String.valueOf(microEvent.getKeys()[i]), microEvent.getRecord_refs()[i], accessType);
            if (rt) {
                assert microEvent.getRecord_refs()[i].getRecord() != null;
            } else {
                return;
            }
        }
    }

    @Override
    public void POST_PROCESS(TxnEvent txnEvent) throws InterruptedException {
        MicroEvent microEvent = (MicroEvent) txnEvent;
        boolean flag = microEvent.READ_EVENT();
        if (flag) {
            READ_POST(microEvent);
        } else {
            WRITE_POST(microEvent);
        }
    }

    protected void READ_POST(MicroEvent event) throws InterruptedException {
        if (checkAbort(event)) {
            collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{0.0}));
        } else {
            collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{(double)event.sum}));
        }
    }

    protected void WRITE_POST(MicroEvent event) throws InterruptedException {
        if (checkAbort(event)) {
            collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{0.0}));
        } else {
            collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{1.0}));
        }
    }

    @Override
    public boolean checkAbort(TxnEvent txnEvent) {
        MicroEvent event = (MicroEvent) txnEvent;
        if (event.READ_EVENT()) {
            return false;
        } else {
            for (int values : event.getValues()){
                if (values < 0) {
                    return true;
                }
            }
        }
        return false;
    }
}
