package applications.bolts.transactional.tp;

import System.sink.helper.ApplicationResult;
import UserApplications.SOURCE_CONTROL;
import applications.events.GlobalSorter;
import applications.events.TxnEvent;
import applications.events.lr.TollProcessingEvent;
import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
import engine.transaction.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltSStore;
import streamprocess.faulttolerance.checkpoint.Status;

import java.util.*;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.NUM_ACCESSES;
import static UserApplications.CONTROL.PARTITION_NUM;
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;

public abstract class TPBolt_SStore extends TransactionalBoltSStore {
    private static final long serialVersionUID = 3147966277697773985L;
    private static final Logger LOG= LoggerFactory.getLogger(TPBolt_SStore.class);
    public TPBolt_SStore(int fid) {
        super(LOG, fid);
        this.configPrefix="tptxn";
        status = new Status();
        this.setStateful();
    }

    @Override
    public void Sort_Lock(int thread_Id) {
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);
        LA_RESETALL(transactionManager, thread_Id);
        //Sort
        if (thread_Id == 0) {
            int[] p_bids = new int[(int) PARTITION_NUM];
            HashMap<Integer, Integer> pids = new HashMap<>();//<partitionId, batchId>
            for (TxnEvent event : GlobalSorter.sortedEvents) {
                if (event instanceof TollProcessingEvent) {
                    parseTollProcessingEvent((TollProcessingEvent) event,pids);
                    event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                    pids.replaceAll((k, v) -> p_bids[k]++);
                } else {
                    throw new UnsupportedOperationException();
                }
                pids.clear();
            }
            GlobalSorter.sortedEvents.clear();
        }
        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
    }
    private void parseTollProcessingEvent(TollProcessingEvent event, HashMap<Integer, Integer> pids) {
        for (int key : event.getKeys()) {
            pids.put(key / partition_delta, 0);
        }
    }

    @Override
    public void LAL(TxnEvent event) throws DatabaseException {
        WRITE_LOCK_AHEAD((TollProcessingEvent) event);
    }
    protected void WRITE_LOCK_AHEAD(TollProcessingEvent event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            transactionManager.lock_ahead("segment_speed",String.valueOf(event.getKeys()[i]),READ_WRITE);
            transactionManager.lock_ahead("segment_cnt",String.valueOf(event.getKeys()[i]),READ_WRITE);
        }
    }

    @Override
    public void PostLAL_Process(TxnEvent event) throws DatabaseException {
        TollProcessingEvent tollProcessingEvent = (TollProcessingEvent) event;
        boolean isAbort = checkAbort(tollProcessingEvent);
        if (!isAbort) {
            TxnContext txnContext = new TxnContext(this.thread_Id, fid, tollProcessingEvent.getBid());
            process_request_noLock(tollProcessingEvent,txnContext,READ_WRITE);
            double latestAvgSpeeds = tollProcessingEvent.getSpeed_value()[0].getRecord().getValues().get(1).getDouble();
            double lav;
            if (latestAvgSpeeds == 0) {//not initialized
                lav = tollProcessingEvent.getSpeedValue();
            } else{
                lav = (latestAvgSpeeds + tollProcessingEvent.getSpeedValue()) / 2;
            }
            tollProcessingEvent.getSpeed_value()[0].getRecord().getValues().get(1).setDouble(lav);
            HashSet cnt_segment = tollProcessingEvent.getCount_value()[0].getRecord().getValues().get(1).getHashSet();
            cnt_segment.add(tollProcessingEvent.getVid());
            for (int i = 0; i < NUM_ACCESSES; i++) {
                tollProcessingEvent.roadSpendValues[i] = tollProcessingEvent.getSpeed_value()[i].getRecord().getValues().get(1).getDouble();
                tollProcessingEvent.cntValues[i] = tollProcessingEvent.getCount_value()[i].getRecord().getValues().get(1).getHashSet().size();
            }
        }
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            keys.add("segment_speed_" + getPartitionId(String.valueOf(tollProcessingEvent.getKeys()[i])));
            keys.add("segment_cnt_" + getPartitionId(String.valueOf(tollProcessingEvent.getKeys()[i])));
        }
        transactionManager.CommitTransaction(keys);
    }

    @Override
    public void POST_PROCESS(TxnEvent txnEvent) throws InterruptedException {
        TollProcessingEvent event = (TollProcessingEvent) txnEvent;
        if(checkAbort(event)){
            event.toll = -1;
            collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{event.toll}));//the tuple is finished.
        } else {
            double spendValue = 0;
            int cntValue = 0;
            for (int i = 0; i < NUM_ACCESSES; i++) {
                spendValue = spendValue + event.roadSpendValues[i];
                cntValue = cntValue + event.cntValues[i];
            }
            //Some UDF function
            if (cntValue <= 50) {
                event.toll = spendValue * 50;
            } else {
                event.toll = spendValue * 50 * (cntValue - 50) * (cntValue - 50);
            }
            collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null,null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{event.toll}));//the tuple is finished.
        }
    }

    private void process_request_noLock(TollProcessingEvent tollProcessingEvent, TxnContext txnContext, MetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "segment_speed", String.valueOf(tollProcessingEvent.getKeys()[i]),tollProcessingEvent.getSpeed_value()[i],READ_WRITE);
            if (rt) {
                assert tollProcessingEvent.getSpeed_value()[i].getRecord() != null;
            } else {
                return;
            }
            rt = transactionManager.SelectKeyRecord_noLock(txnContext, "segment_cnt", String.valueOf(tollProcessingEvent.getKeys()[i]),tollProcessingEvent.getCount_value()[i],READ_WRITE);
            if (rt) {
                assert tollProcessingEvent.getCount_value()[i].getRecord() != null;
            } else {
                return;
            }
        }
    }

    @Override
    public boolean checkAbort(TxnEvent txnEvent) {
        TollProcessingEvent event = (TollProcessingEvent) txnEvent;
        for (double spendValue : event.roadSpendValues) {
            if (spendValue > 350) {
                return true;
            }
        }
        return false;
    }
}
