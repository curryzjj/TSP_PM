package applications.bolts.transactional.sl;

import System.measure.MeasureTools;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.SL.TransactionResult;
import applications.events.TxnEvent;
import engine.Exception.DatabaseException;
import engine.transaction.TxnContext;
import engine.transaction.function.Condition;
import engine.transaction.function.DEC;
import engine.transaction.function.INC;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.controller.output.Determinant.InsideDeterminant;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.faulttolerance.clr.CausalService;

import java.util.ArrayList;
import java.util.List;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class SLBolt_TStream_Conventional extends TransactionalBoltTStream {
    private static final long serialVersionUID = 1259273311305261364L;
    List<TxnEvent> EventsHolder = new ArrayList<>();
    public SLBolt_TStream_Conventional(int fid) {
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
    }

    protected void DeterminantDepositRequestConstruct(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEventList.get(markerId).contains(event.getBid())) {
                    event.txnContext.isAbort.compareAndSet(false, true);
                    return;
                }
            }
        }
        Deposit_Request_Construct(event, txnContext);
    }
    protected void DeterminantTransferRequestConstruct(TransactionEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c : this.causalService.values()) {
                InsideDeterminant insideDeterminant = c.insideDeterminantList.get(markerId).get(event.getBid());
                if (insideDeterminant.isAbort) {
                    event.txnContext.isAbort.compareAndSet(false, true);
                    return;
                } else {
                    event.src_account_value.setRecord(insideDeterminant.ackValues.get(event.getSourceAccountId()));
                    event.src_asset_value.setRecord(insideDeterminant.ackValues.get(event.getSourceBookEntryId()));
                }
            }
        }
        TransferRequestConstruct(event, txnContext);
    }

    @Override
    protected void REQUEST_POST() throws InterruptedException {
        //deduplication at upstream
        if (this.markerId > recoveryId) {
            for (TxnEvent event:EventsHolder){
                TransactionResult transactionResult = null;
                int targetId;
                if (enable_determinants_log) {
                    MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
                    InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                    if (event.txnContext.isAbort.get()) {
                        insideDeterminant.setAbort(true);
                        MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                        targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, insideDeterminant, event.getTimestamp());//the tuple is finished.
                    } else {
                        if(event instanceof TransactionEvent){
                            transactionResult = ((TransactionEvent) event).transaction_result;
                            insideDeterminant.setAckValues(((TransactionEvent)event).getSourceAccountId(), ((TransactionEvent) event).src_account_value.getRecord());
                            insideDeterminant.setAckValues(((TransactionEvent)event).getSourceBookEntryId(), ((TransactionEvent) event).src_asset_value.getRecord());
                        }else{
                            transactionResult = ((DepositEvent) event).transactionResult;
                        }
                        MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                        targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, insideDeterminant, event.getTimestamp());//the tuple is finished.
                    }
                } else {
                    if (event.txnContext.isAbort.get()) {
                        targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), false, null, event.getTimestamp());//the tuple is finished.
                    } else {
                        targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, event.getTimestamp());//the tuple is finished.
                    }
                }
                if (enable_upstreamBackup) {
                    MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
                    this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, transactionResult);
                    MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
                }
            }
            MeasureTools.Upstream_backup_finish_acc(this.executor.getExecutorID());
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
            event.transaction_result = new TransactionResult(event.getBid(), event.getTimestamp(), false);
        } else {
            event.transaction_result = new TransactionResult(event.getBid(), event.getTimestamp(), event.success[0], event.src_account_value.getRecord().getValues().get(1).getLong(), event.src_asset_value.getRecord().getValues().get(1).getLong());
        }
    }
    protected void DEPOSITE_REQUEST_CORE(DepositEvent event) {
        event.transactionResult = new TransactionResult(event.getBid(), event.getTimestamp(), event.success[0]);
    }
}
