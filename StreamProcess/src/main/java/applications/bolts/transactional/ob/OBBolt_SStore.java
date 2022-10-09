package applications.bolts.transactional.ob;

import System.sink.helper.ApplicationResult;
import UserApplications.SOURCE_CONTROL;
import applications.events.GlobalSorter;
import applications.events.TxnEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import engine.Exception.DatabaseException;
import engine.Meta.MetaTypes;
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
import static engine.Meta.MetaTypes.AccessType.READ_WRITE;

public abstract class OBBolt_SStore extends TransactionalBoltSStore {
    private static final long serialVersionUID = 7126681465539745792L;
    private static final Logger LOG= LoggerFactory.getLogger(OBBolt_SStore.class);
    public OBBolt_SStore(int fid) {
        super(LOG, fid);
        this.configPrefix="obtxn";
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
                    if (event instanceof BuyingEvent) {
                        parseBuyEvent((BuyingEvent) event, pids);
                    } else if (event instanceof AlertEvent) {
                        parseAlertEvent((AlertEvent) event, pids);
                    } else if (event instanceof ToppingEvent) {
                        parseTopEvent((ToppingEvent) event, pids);
                    } else {
                        throw new UnsupportedOperationException();
                    }
                    event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                    pids.replaceAll((k, v) -> p_bids[k]++);
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
    private void parseBuyEvent(BuyingEvent event, HashMap<Integer, Integer> pids) {
        for (int key : event.getItemId()) {
            pids.put(key / partition_delta, 0);
        }
    }
    private void parseAlertEvent(AlertEvent event, HashMap<Integer, Integer> pids) {
        for (int key : event.getItemId()) {
            pids.put(key / partition_delta, 0);
        }
    }
    private void parseTopEvent(ToppingEvent event, HashMap<Integer, Integer> pids) {
        for (int key : event.getItemId()) {
            pids.put(key / partition_delta, 0);
        }
    }

    @Override
    public void LAL(TxnEvent event) throws DatabaseException {
        WRITE_LOCK_AHEAD(event);
    }
    protected void WRITE_LOCK_AHEAD(TxnEvent event) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            if (event instanceof BuyingEvent) {
                transactionManager.lock_ahead("goods", String.valueOf(((BuyingEvent) event).getItemId()[i]), READ_WRITE);
            } else if (event instanceof AlertEvent) {
                transactionManager.lock_ahead("goods", String.valueOf(((AlertEvent) event).getItemId()[i]), READ_WRITE);
            } else if (event instanceof ToppingEvent) {
                transactionManager.lock_ahead("goods", String.valueOf(((ToppingEvent) event).getItemId()[i]), READ_WRITE);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Override
    public void PostLAL_Process(TxnEvent txnEvent, boolean snapshotLock) throws DatabaseException {
        List<String> keys = new ArrayList<>();
        if (txnEvent instanceof BuyingEvent) {
            BuyingEvent event = (BuyingEvent) txnEvent;
            if (checkAbort(txnEvent)) {
                event.biding_result = false;
            } else {
                TxnContext txnContext = new TxnContext(this.thread_Id, fid, event.getBid());
                process_buy_request_noLock(event, txnContext);
            }
            for (int i = 0; i < NUM_ACCESSES; ++i) {
                keys.add("goods_" + getPartitionId(String.valueOf(event.getItemId()[i])));
            }
        } else if (txnEvent instanceof AlertEvent) {
            AlertEvent event = (AlertEvent) txnEvent;
            if (checkAbort(txnEvent)){
                event.alert_result = false;
            } else {
                TxnContext txnContext = new TxnContext(this.thread_Id, fid, event.getBid());
                process_alert_request_noLock(event,txnContext);
            }
            for (int i = 0; i < NUM_ACCESSES; ++i) {
                keys.add("goods_" + getPartitionId(String.valueOf(event.getItemId()[i])));
            }
        } else if (txnEvent instanceof ToppingEvent) {
            ToppingEvent event = (ToppingEvent) txnEvent;
            if(checkAbort(txnEvent)){
                event.topping_result = false;
            } else {
                TxnContext txnContext = new TxnContext(this.thread_Id, fid, event.getBid());
                process_top_request_noLock(event, txnContext);
            }
            for (int i = 0; i < NUM_ACCESSES; ++i) {
                keys.add("goods_" + getPartitionId(String.valueOf(event.getItemId()[i])));
            }
        }
        if (snapshotLock) {
            this.syncSnapshot();
        }
        transactionManager.CommitTransaction(keys);
    }
    private void process_top_request_noLock(ToppingEvent toppingEvent, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            SchemaRecordRef schemaRecordRef = new SchemaRecordRef();
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "goods",
                    String.valueOf(toppingEvent.getItemId()[i]), schemaRecordRef, MetaTypes.AccessType.READ_WRITE);
            if (rt) {
                assert schemaRecordRef.getRecord() != null;
                schemaRecordRef.getRecord().getValues().get(2).setLong(schemaRecordRef.getRecord().getValues().get(2).getLong() + toppingEvent.getItemTopUp()[i]);
            } else {
                return;
            }
        }
    }
    private void process_alert_request_noLock(AlertEvent alertEvent, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            SchemaRecordRef schemaRecordRef = new SchemaRecordRef();
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "goods",
                    String.valueOf(alertEvent.getItemId()[i]), schemaRecordRef, MetaTypes.AccessType.WRITE_ONLY);
            if (rt) {
                assert schemaRecordRef.getRecord() != null;
                schemaRecordRef.getRecord().getValues().get(1).setLong(alertEvent.getAsk_price()[i]);
            } else {
                return;
            }
        }
    }
    private void process_buy_request_noLock(BuyingEvent buyingEvent, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; ++i){
            SchemaRecordRef schemaRecordRef = new SchemaRecordRef();
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "goods",
                    String.valueOf(buyingEvent.getItemId()[i]), schemaRecordRef, READ_WRITE);
            if (rt) {
                assert schemaRecordRef.getRecord() != null;
                // check the preconditions
                long askPrice = schemaRecordRef.getRecord().getValues().get(1).getLong();//price
                long left_qty = schemaRecordRef.getRecord().getValues().get(2).getLong();//available qty;
                long bidPrice = buyingEvent.getBidPrice()[i];
                long bid_qty = buyingEvent.getBidQty()[i];
                if (bidPrice > askPrice && bid_qty < left_qty ) {
                    schemaRecordRef.getRecord().getValues().get(2).setLong(left_qty - bid_qty);//new quantity.
                } else {
                   buyingEvent.biding_result = false;
                }
            } else {
                return;
            }
        }
    }

    @Override
    public void POST_PROCESS(TxnEvent txnEvent) throws InterruptedException {
        if (txnEvent instanceof BuyingEvent) {
            BUYING_POST((BuyingEvent) txnEvent);
        } else if (txnEvent instanceof AlertEvent) {
            ALERT_POST((AlertEvent) txnEvent);
        } else if (txnEvent instanceof ToppingEvent) {
            TOPPING_POST((ToppingEvent) txnEvent);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private void TOPPING_POST(ToppingEvent toppingEvent) throws InterruptedException {
        if (toppingEvent.topping_result) {
            collector.emit_single(DEFAULT_STREAM_ID, toppingEvent.getBid(), true, null,null, toppingEvent.getTimestamp(),new ApplicationResult(toppingEvent.getBid(), new Double[]{1.0}));//the tuple is finished finally.
        } else {
            collector.emit_single(DEFAULT_STREAM_ID, toppingEvent.getBid(), false, null,null, toppingEvent.getTimestamp(),new ApplicationResult(toppingEvent.getBid(), new Double[]{0.0}));//the tuple is finished finally.
        }
    }
    private void ALERT_POST(AlertEvent alertEvent) throws InterruptedException {
        if (alertEvent.alert_result) {
            collector.emit_single(DEFAULT_STREAM_ID, alertEvent.getBid(), true, null,null, alertEvent.getTimestamp(),new ApplicationResult(alertEvent.getBid(), new Double[]{1.0}));//the tuple is finished finally.
        } else {
            collector.emit_single(DEFAULT_STREAM_ID, alertEvent.getBid(), false, null,null, alertEvent.getTimestamp(),new ApplicationResult(alertEvent.getBid(), new Double[]{0.0}));//the tuple is finished finally.
        }
    }
    private void BUYING_POST(BuyingEvent buyingEvent) throws InterruptedException {
        if (buyingEvent.biding_result) {
            collector.emit_single(DEFAULT_STREAM_ID, buyingEvent.getBid(), true, null,null, buyingEvent.getTimestamp(),new ApplicationResult(buyingEvent.getBid(), new Double[]{1.0}));//the tuple is finished finally.
        } else {
            collector.emit_single(DEFAULT_STREAM_ID, buyingEvent.getBid(), false, null,null, buyingEvent.getTimestamp(),new ApplicationResult(buyingEvent.getBid(), new Double[]{0.0}));//the tuple is finished finally.
        }
    }

    @Override
    public boolean checkAbort(TxnEvent txnEvent) {
        if (txnEvent instanceof BuyingEvent) {
            BuyingEvent event = (BuyingEvent) txnEvent;
            for (int i = 0; i < NUM_ACCESSES; ++i){
                if (event.getBidQty()[i] < 0) {
                    return true;
                }
            }
        } else if (txnEvent instanceof AlertEvent) {
            AlertEvent event = (AlertEvent) txnEvent;
            for (int i = 0; i < NUM_ACCESSES; ++i){
                if (event.getAsk_price()[i] == -1) {
                    return true;
                }
            }
        } else if (txnEvent instanceof ToppingEvent) {
            ToppingEvent event = (ToppingEvent) txnEvent;
            for (int i = 0; i < NUM_ACCESSES; ++i){
                if (event.getItemTopUp()[i] < 0) {
                    return true;
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }
        return false;
    }
}
