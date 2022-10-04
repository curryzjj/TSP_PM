package applications.bolts.transactional.sl;

import System.sink.helper.ApplicationResult;
import UserApplications.SOURCE_CONTROL;
import applications.bolts.transactional.gs.GSBolt_SStore;
import applications.events.GlobalSorter;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.TxnEvent;
import applications.events.gs.MicroEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import engine.Exception.DatabaseException;
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

public abstract class SLBolt_Store extends TransactionalBoltSStore {
    private static final long serialVersionUID = 8237366192292475213L;
    private static final Logger LOG= LoggerFactory.getLogger(SLBolt_Store.class);
    public SLBolt_Store(int fid) {
        super(LOG, fid);
        this.configPrefix="sltxn";
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
                    if (event instanceof TransactionEvent) {
                        parseTransferEvent((TransactionEvent) event, pids);
                    } else if (event instanceof DepositEvent) {
                        parseDepositEvent((DepositEvent) event, pids);
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
    private void parseTransferEvent(TransactionEvent transactionEvent, HashMap<Integer, Integer> pids) {
        pids.put(Integer.parseInt(transactionEvent.getSourceAccountId()) / partition_delta, 0);
        pids.put(Integer.parseInt(transactionEvent.getSourceBookEntryId()) / partition_delta, 0);
        pids.put(Integer.parseInt(transactionEvent.getTargetAccountId()) / partition_delta, 0);
        pids.put(Integer.parseInt(transactionEvent.getTargetBookEntryId()) / partition_delta, 0);
    }
    private void parseDepositEvent(DepositEvent depositEvent, HashMap<Integer, Integer> pids) {
        pids.put(Integer.parseInt(depositEvent.getAccountId()) / partition_delta, 0);
        pids.put(Integer.parseInt(depositEvent.getBookEntryId()) / partition_delta, 0);
    }

    @Override
    public void LAL(TxnEvent event) throws DatabaseException {
        WRITE_LOCK_AHEAD(event);
    }
    protected void WRITE_LOCK_AHEAD(TxnEvent event) throws DatabaseException {
        if (event instanceof TransactionEvent) {
            TransactionEvent transactionEvent = (TransactionEvent) event;
            transactionManager.lock_ahead("T_accounts", transactionEvent.getSourceAccountId(), READ_WRITE);
            transactionManager.lock_ahead("T_accounts", transactionEvent.getTargetAccountId(), READ_WRITE);
            transactionManager.lock_ahead("T_assets", transactionEvent.getSourceBookEntryId(), READ_WRITE);
            transactionManager.lock_ahead("T_assets", transactionEvent.getTargetBookEntryId(), READ_WRITE);
        } else {
            DepositEvent depositEvent = (DepositEvent) event;
            transactionManager.lock_ahead("T_accounts", depositEvent.getAccountId(), READ_WRITE);
            transactionManager.lock_ahead("T_assets", depositEvent.getBookEntryId(), READ_WRITE);
        }
    }

    @Override
    public void PostLAL_Process(TxnEvent event) throws DatabaseException {
        List<String> keys = new ArrayList<>();
        if (event instanceof TransactionEvent) {
            TransactionEvent transactionEvent = (TransactionEvent) event;
            TxnContext txnContext = new TxnContext(this.thread_Id, fid, event.getBid());
            event.setTxnContext(txnContext);
            process_Transfer_request_noLock(transactionEvent, txnContext);
            keys.add("T_accounts_" + getPartitionId(transactionEvent.getSourceAccountId()));
            keys.add("T_accounts_" + getPartitionId(transactionEvent.getTargetAccountId()));
            keys.add("T_assets_" + getPartitionId(transactionEvent.getSourceBookEntryId()));
            keys.add("T_assets_" + getPartitionId(transactionEvent.getTargetBookEntryId()));
        } else {
            DepositEvent depositEvent = (DepositEvent) event;
            TxnContext txnContext = new TxnContext(this.thread_Id, fid, event.getBid());
            event.setTxnContext(txnContext);
            process_Deposit_request_noLock(depositEvent, txnContext);
            keys.add("T_accounts_" + getPartitionId(depositEvent.getAccountId()));
            keys.add("T_assets_" + getPartitionId(depositEvent.getBookEntryId()));
        }
        transactionManager.CommitTransaction(keys);
    }
    private void process_Transfer_request_noLock(TransactionEvent event, TxnContext txnContext) throws DatabaseException {
        SchemaRecordRef targetAccountRef = new SchemaRecordRef();
        SchemaRecordRef targetBookRef = new SchemaRecordRef();
        transactionManager.SelectKeyRecord_noLock(txnContext, "T_accounts" ,event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "T_accounts" ,event.getTargetAccountId(), targetAccountRef, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "T_assets" ,event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "T_assets" ,event.getTargetBookEntryId(), targetBookRef, READ_WRITE);
        //To contron the abort ratio, wo modify the violation of consistency property
        if (event.getMinAccountBalance() > 0) {
            long sourceBalance = event.src_account_value.getRecord().getValues().get(1).getLong();
            event.src_account_value.getRecord().getValues().get(1).decLong(event.getAccountTransfer());
            targetAccountRef.getRecord().getValues().get(1).incLong((long) (event.getAccountTransfer() + sourceBalance * 0.1));
            sourceBalance = event.src_asset_value.getRecord().getValues().get(1).getLong();
            event.src_asset_value.getRecord().getValues().get(1).decLong(event.getBookEntryTransfer());
            targetBookRef.getRecord().getValues().get(1).incLong((long) (event.getBookEntryTransfer() + sourceBalance * 0.1));
            event.txnContext.isAbort.set(false);
        } else {
            event.txnContext.isAbort.set(true);
        }
    }
    private void process_Deposit_request_noLock(DepositEvent event, TxnContext txnContext) throws DatabaseException {
        SchemaRecordRef sourceAccountRef = new SchemaRecordRef();
        SchemaRecordRef sourceBookRef = new SchemaRecordRef();
        transactionManager.SelectKeyRecord_noLock(txnContext, "T_accounts" ,event.getAccountId(), sourceAccountRef, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "T_assets" ,event.getBookEntryId(), sourceBookRef, READ_WRITE);
        if (event.getAccountTransfer() > 0 && event.getBookEntryTransfer() > 0) {
            sourceAccountRef.getRecord().getValues().get(1).incLong(event.getAccountTransfer());
            sourceBookRef.getRecord().getValues().get(1).incLong(event.getBookEntryTransfer());
            event.txnContext.isAbort.set(false);
        } else {
            event.txnContext.isAbort.set(true);
        }
    }

    @Override
    public void POST_PROCESS(TxnEvent txnEvent) throws InterruptedException {
        if (txnEvent instanceof TransactionEvent) {
            TransactionEvent event = (TransactionEvent) txnEvent;
            if (event.txnContext.isAbort.get()) {
                collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished.
            } else {
                collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{(double) event.src_account_value.getRecord().getValues().get(1).getLong(), (double) event.src_account_value.getRecord().getValues().get(1).getLong()}));//the tuple is finished.
            }
        } else {
            DepositEvent event = (DepositEvent) txnEvent;
            if (event.txnContext.isAbort.get()) {
                collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished.
            } else {
                collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished.
            }
        }
    }

    @Override
    public boolean checkAbort(TxnEvent txnEvent) {
        return false;
    }
}
