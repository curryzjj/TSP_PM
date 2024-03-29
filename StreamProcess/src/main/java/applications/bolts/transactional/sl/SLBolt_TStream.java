package applications.bolts.transactional.sl;
import System.measure.MeasureTools;
import System.sink.helper.ApplicationResult;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.SL.TransactionResult;
import applications.events.TxnEvent;
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
import java.util.Collections;
import java.util.List;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;
import static applications.events.DeserializeEventHelper.deserializeEvent;

public abstract class SLBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(SLBolt_TStream.class);
    private static final long serialVersionUID = -5333749446740104376L;
    List<TxnEvent> EventsHolder = new ArrayList<>();
    public SLBolt_TStream(int fid) {
        super(LOG, fid);
        this.configPrefix = "tpsl";
        status = new Status();
        this.setStateful();
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        MeasureTools.Transaction_construction_begin(this.thread_Id, System.nanoTime());
        TxnContext txnContext = new TxnContext(thread_Id, this.fid, in.getBID());
        TxnEvent event = (TxnEvent) in.getValue(0);
        event.setTxnContext(txnContext);
        if (event instanceof DepositEvent) {
            if (enable_determinants_log) {
                DeterminantDepositRequestConstruct((DepositEvent) event, txnContext);
            } else {
                Deposit_Request_Construct((DepositEvent) event, txnContext);
            }
        } else {
            if (enable_determinants_log) {
                DeterminantTransferRequestConstruct((TransactionEvent) event, txnContext);
            } else {
                TransferRequestConstruct((TransactionEvent) event, txnContext);
            }
        }
        MeasureTools.Transaction_construction_acc(this.thread_Id, System.nanoTime());
    }
    protected void Deposit_Request_Construct(DepositEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.Asy_ModifyRecord(txnContext,"T_accounts",event.getAccountId(), new INC(event.getAccountTransfer()));
        transactionManager.Asy_ModifyRecord(txnContext,"T_assets",event.getBookEntryId(), new INC(event.getBookEntryTransfer()));
        EventsHolder.add(event);
        if (enable_recovery_dependency) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            String[] keys = new String[]{event.getAccountId(), event.getBookEntryId()};
            this.updateRecoveryDependency(keys, true);
            MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
        }
    }
    protected void TransferRequestConstruct(TransactionEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        String[] accTable = new String[]{"T_accounts"};
        String[] astTable = new String[]{"T_assets"};
        String[] accID = new String[]{event.getSourceAccountId()};
        String[] astID = new String[]{event.getSourceBookEntryId()};
        transactionManager.Asy_ModifyRecord(txnContext,
                "T_accounts",
                event.getSourceAccountId(),
                new DEC(event.getAccountTransfer()),
                accTable,
                accID,//condition source, condition id.
                new Condition(
                        event.getMinAccountBalance(),
                        event.getAccountTransfer()),
                event.success);
        transactionManager.Asy_ModifyRecord(txnContext,
                "T_assets",
                event.getSourceBookEntryId(),
                new DEC(event.getBookEntryTransfer()),
                astTable,
                astID,
                new Condition(event.getMinAccountBalance(),
                        event.getBookEntryTransfer()),
                event.success);   //asynchronously return.
        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "T_accounts",
                event.getTargetAccountId(),
                event.src_account_value,//to be fill up.
                new INC(event.getAccountTransfer()),
                accTable,
                accID,//condition source, condition id.
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer()),
                event.success);          //asynchronously return.
        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "T_assets",
                event.getTargetBookEntryId(),
                event.src_asset_value,
                new INC(event.getBookEntryTransfer()),
                astTable,
                astID,
                new Condition(event.getMinAccountBalance(),
                        event.getBookEntryTransfer()),
                event.success);   //asynchronously return.
        EventsHolder.add(event);
        if (enable_recovery_dependency) {
        MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
        String[] keys = new String[]{event.getSourceAccountId(),event.getTargetAccountId(),event.getSourceBookEntryId(), event.getTargetBookEntryId()};
        this.updateRecoveryDependency(keys,true);
        MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
        }
    }

    protected void DeterminantDepositRequestConstruct(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.getAbortEventsByMarkerId(event.getBid()).contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false,true);
                    return;
                }
            }
            if (this.recoveryPartitionIds.contains(this.getPartitionId(event.getAccountId()))) {
                transactionManager.Asy_ModifyRecord(txnContext,"T_accounts",event.getAccountId(),new INC(event.getAccountTransfer()));
            }
            if (this.recoveryPartitionIds.contains(this.getPartitionId(event.getBookEntryId()))) {
                transactionManager.Asy_ModifyRecord(txnContext,"T_assets",event.getBookEntryId(),new INC(event.getBookEntryTransfer()));
            }
        } else {
            Deposit_Request_Construct(event, txnContext);
        }
    }
    protected void DeterminantTransferRequestConstruct(TransactionEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.getAbortEventsByMarkerId(event.getBid()).contains(event.getBid())){
                    event.txnContext.isAbort.compareAndSet(false,true);
                    return;
                } else if (c.getInsideDeterminantByMarkerId(event.getBid()).containsKey(event.getBid())) {
                    InsideDeterminant insideDeterminant = c.getInsideDeterminantByMarkerId(event.getBid()).get(event.getBid());
                    if (insideDeterminant.ackValues.containsKey(event.getSourceAccountId())) {
                        event.src_account_value.setRecord(insideDeterminant.ackValues.get(event.getSourceBookEntryId()));
                    }
                    if (insideDeterminant.ackValues.containsKey(event.getSourceBookEntryId())) {
                        event.src_asset_value.setRecord(insideDeterminant.ackValues.get(event.getSourceBookEntryId()));
                    }
                }
            }
            String[] accTable = new String[]{"T_accounts"};
            String[] astTable = new String[]{"T_assets"};
            String[] accID = new String[]{event.getSourceAccountId()};
            String[] astID = new String[]{event.getSourceBookEntryId()};
            if (this.recoveryPartitionIds.contains(this.getPartitionId(event.getSourceAccountId()))) {
                transactionManager.Asy_ModifyRecord(txnContext,
                        "T_accounts",
                        event.getSourceAccountId(),
                        new DEC(event.getAccountTransfer()),
                        accTable,
                        accID,//condition source, condition id.
                        new Condition(
                                event.getMinAccountBalance(),
                                event.getAccountTransfer()),
                        event.success);
            }
            if (this.recoveryPartitionIds.contains(this.getPartitionId(event.getSourceBookEntryId()))) {
                transactionManager.Asy_ModifyRecord(txnContext,
                        "T_assets",
                        event.getSourceBookEntryId(),
                        new DEC(event.getBookEntryTransfer()),
                        astTable,
                        astID,
                        new Condition(event.getMinAccountBalance(),
                                event.getBookEntryTransfer()),
                        event.success);   //asynchronously return.
            }
            if (this.recoveryPartitionIds.contains(this.getPartitionId(event.getTargetAccountId()))) {
                transactionManager.Asy_ModifyRecord_Read(txnContext,
                        "T_accounts",
                        event.getTargetAccountId(),
                        event.src_account_value,//to be fill up.
                        new INC(event.getAccountTransfer()),
                        accTable,
                        accID,//condition source, condition id.
                        new Condition(event.getMinAccountBalance(),
                                event.getAccountTransfer()),
                        event.success);
            }
            if (this.recoveryPartitionIds.contains(this.getPartitionId(event.getTargetBookEntryId()))) {
                transactionManager.Asy_ModifyRecord_Read(txnContext,
                        "T_assets",
                        event.getTargetBookEntryId(),
                        event.src_asset_value,
                        new INC(event.getBookEntryTransfer()),
                        astTable,
                        astID,
                        new Condition(event.getMinAccountBalance(),
                                event.getBookEntryTransfer()),
                        event.success);
            }
        } else {
            TransferRequestConstruct(event, txnContext);
        }
    }

    protected void CommitOutsideDeterminant(long markId) throws DatabaseException, InterruptedException {
        if ((enable_key_based || this.executor.isFirst_executor()) && !this.causalService.isEmpty()) {
            for (CausalService c:this.causalService.values()) {
                if (c.outsideDeterminantList.get(markId) != null){
                    List<Long> isCommit = new ArrayList<>();
                    for (OutsideDeterminant outsideDeterminant:c.outsideDeterminantList.get(markId)) {
                        TxnEvent event = deserializeEvent(outsideDeterminant.outSideEvent);
                        if (!isCommit.contains(event.getBid())) {
                            TxnContext txnContext = new TxnContext(thread_Id,this.fid,event.getBid());
                            event.setTxnContext(txnContext);
                            if (event instanceof DepositEvent) {
                                DeterminantDepositRequestConstruct((DepositEvent) event, txnContext);
                            } else {
                                if (outsideDeterminant.ackValues.get(((TransactionEvent) event).getSourceAccountId()) != null) {
                                    ((TransactionEvent) event).src_account_value.setRecord(outsideDeterminant.ackValues.get(((TransactionEvent) event).getSourceAccountId()));
                                }
                                if (outsideDeterminant.ackValues.get(((TransactionEvent) event).getSourceBookEntryId()) != null) {
                                    ((TransactionEvent) event).src_asset_value.setRecord(outsideDeterminant.ackValues.get(((TransactionEvent) event).getSourceBookEntryId()));
                                }
                                DeterminantTransferRequestConstruct((TransactionEvent) event, txnContext);
                            }
                            isCommit.add(event.getBid());
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void REQUEST_POST() throws InterruptedException {
        //deduplication at upstream
        if (this.markerId > recoveryId) {
            for (TxnEvent event:EventsHolder){
                if (event instanceof TransactionEvent){
                    TRANSFER_POST((TransactionEvent) event);
                } else {
                    DEPOSIT_POST((DepositEvent) event);
                }
            }
            if (enable_upstreamBackup) {
                MeasureTools.Upstream_backup_finish_acc(this.executor.getExecutorID());
            }
        }
    }

    @Override
    protected void REQUEST_CORE() throws InterruptedException {
        if (this.markerId > recoveryId) {
            for (TxnEvent event : EventsHolder) {
                if (event instanceof TransactionEvent) {
                    TRANSFER_REQUEST_CORE((TransactionEvent) event);
                } else {
                    DEPOSITE_REQUEST_CORE((DepositEvent) event);
                }
            }
        }
    }

    private void TRANSFER_REQUEST_CORE(TransactionEvent event) {
        if (event.txnContext.isAbort.get()) {
            event.transaction_result = new TransactionResult(event.getBid(), false);
        } else {
            event.transaction_result = new TransactionResult(event.getBid(),  event.success[0], event.src_account_value.getRecord().getValues().get(1).getLong(), event.src_asset_value.getRecord().getValues().get(1).getLong());
        }
    }
    protected void DEPOSITE_REQUEST_CORE(DepositEvent event) {
        event.transactionResult = new TransactionResult(event.getBid(), event.success[0]);
    }

    protected void TRANSFER_POST(TransactionEvent event) throws InterruptedException {
        TransactionResult transactionResult = event.transaction_result;
        int targetId;
        if (enable_determinants_log) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            if (event.txnContext.isAbort.get()) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, insideDeterminant,null, event.getTimestamp(), new ApplicationResult(event.getBid(),new Double[]{0.0}));
            } else {
                OutsideDeterminant outsideDeterminant = new OutsideDeterminant();
                InsideDeterminant insideDeterminant = null;
                String[] keys = new String[]{event.getSourceAccountId(), event.getSourceBookEntryId(),
                        event.getTargetAccountId(), event.getTargetBookEntryId()};
                if (this.getPartitionId(keys[1]) != event.getPid()) {
                    outsideDeterminant.setTargetPartitionId(this.getPartitionId(keys[1]));
                }
                if (this.getPartitionId(keys[2]) != event.getPid()) {
                    outsideDeterminant.setTargetPartitionId(this.getPartitionId(keys[2]));
                    outsideDeterminant.setAckValues(keys[0], event.src_account_value.getRecord());
                }
                if (this.getPartitionId(keys[3]) != event.getPid()) {
                    outsideDeterminant.setTargetPartitionId(this.getPartitionId(keys[3]));
                    outsideDeterminant.setAckValues(keys[1], event.src_asset_value.getRecord());
                } else {
                    if (this.getPartitionId(keys[1]) != event.getPid()) {
                        insideDeterminant = new InsideDeterminant(event.getBid(),event.getPid());
                        insideDeterminant.setAckValues(keys[1], event.src_asset_value.getRecord());
                    }
                }
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                if (!outsideDeterminant.targetPartitionIds.isEmpty()) {
                    outsideDeterminant.setOutSideEvent(event.toString());
                    targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, insideDeterminant, outsideDeterminant, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{(double) event.src_account_value.getRecord().getValues().get(1).getLong(), (double) event.src_account_value.getRecord().getValues().get(1).getLong()}));//the tuple is finished.
                } else {
                    targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, insideDeterminant, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{(double) event.src_account_value.getRecord().getValues().get(1).getLong(), (double) event.src_account_value.getRecord().getValues().get(1).getLong()}));//the tuple is finished.
                }
            }
        } else {
            if (event.txnContext.isAbort.get()) {
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished.
            } else {
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(), new ApplicationResult(event.getBid(), new Double[]{(double) event.src_account_value.getRecord().getValues().get(1).getLong(), (double) event.src_account_value.getRecord().getValues().get(1).getLong()}));//the tuple is finished.
            }
        }
        if (enable_upstreamBackup) {
            MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
            this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, transactionResult);
            MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
        }
    }
    protected void DEPOSIT_POST(DepositEvent event) throws InterruptedException {
        TransactionResult transactionResult = event.transactionResult;
        int targetId;
        if (enable_determinants_log) {
            MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
            if (event.txnContext.isAbort.get()) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, insideDeterminant, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{0.0}));
            } else {
                OutsideDeterminant outsideDeterminant = new OutsideDeterminant();
                String[] keys = new String[]{((DepositEvent)event).getAccountId(), ((DepositEvent)event).getBookEntryId()};
                for (String id : keys) {
                    if (this.getPartitionId(id) != event.getPid()) {
                        outsideDeterminant.setTargetPartitionId(this.getPartitionId(id));
                    }
                }
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                if (!outsideDeterminant.targetPartitionIds.isEmpty()) {
                    outsideDeterminant.setOutSideEvent(event.toString());
                    targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, outsideDeterminant, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished.
                } else {
                    targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished.
                }
            }
        } else {
            if (event.txnContext.isAbort.get()) {
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{0.0}));//the tuple is finished.
            } else {
                targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, null, event.getTimestamp(),new ApplicationResult(event.getBid(), new Double[]{1.0}));//the tuple is finished.
            }
        }
        if (enable_upstreamBackup) {
            MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
            this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, transactionResult);
            MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
        }
    }
}
