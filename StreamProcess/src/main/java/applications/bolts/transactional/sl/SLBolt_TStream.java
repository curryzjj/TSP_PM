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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.controller.output.Determinant.InsideDeterminant;
import streamprocess.controller.output.Determinant.OutsideDeterminant;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.faulttolerance.clr.CausalService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public abstract class SLBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(SLBolt_TStream.class);
    List<TxnEvent> EventsHolder = new ArrayList<>();
    public SLBolt_TStream(int fid) {
        super(LOG, fid);
        this.configPrefix = "tpsl";
        status = new Status();
        this.setStateful();
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id, this.fid,in.getBID());
        TxnEvent event = (TxnEvent) in.getValue(0);
        if (event instanceof DepositEvent) {
            if (enable_determinants_log) {
                DeterminantDepositRequestConstruct((DepositEvent) event, txnContext);
            } else {
                Deposit_Request_Construct((DepositEvent) event, txnContext,false);
            }
        } else {
            if (enable_determinants_log) {
                DeterminantTransferRequestConstruct((TransactionEvent) event, txnContext);
            } else {
                TransferRequestConstruct((TransactionEvent) event, txnContext,false);
            }
        }
    }
    protected boolean Deposit_Request_Construct(DepositEvent event, TxnContext txnContext, boolean isReConstruct) throws DatabaseException, InterruptedException {
        boolean flag = transactionManager.Asy_ModifyRecord(txnContext,"T_accounts",event.getAccountId(),new INC(event.getAccountTransfer()));
        if(!flag){
            int targetId;
            if (enable_determinants_log) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                targetId = collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,insideDeterminant,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            } else {
                targetId = collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,null, event.getTimestamp());//the tuple is finished.//the tuple is abort.
            }
            if (enable_upstreamBackup) {
                this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, event.cloneEvent());
            }
            return false;
        }
        transactionManager.Asy_ModifyRecord(txnContext,"T_assets",event.getBookEntryId(),new INC(event.getBookEntryTransfer()));
        if(!isReConstruct){
            EventsHolder.add(event);
            if (enable_recovery_dependency) {
                MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
                String[] keys = new String[]{event.getAccountId(), event.getBookEntryId()};
                this.updateRecoveryDependency(keys,true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
            }
        }
        return true;
    }
    protected boolean TransferRequestConstruct(TransactionEvent event, TxnContext txnContext, boolean isReConstruct) throws DatabaseException, InterruptedException {
        String[] accTable = new String[]{"T_accounts"};
        String[] astTable = new String[]{"T_assets"};
        String[] accID = new String[]{event.getSourceAccountId()};
        String[] astID = new String[]{event.getSourceBookEntryId()};
        boolean flag = transactionManager.Asy_ModifyRecord(txnContext,
                "T_accounts",
                event.getSourceAccountId(),
                new DEC(event.getAccountTransfer()),
                accTable,
                accID,//condition source, condition id.
                new Condition(
                        event.getMinAccountBalance(),
                        event.getAccountTransfer()),
                event.success);
        if(!flag){
            int targetId;
            if (enable_determinants_log) {
                InsideDeterminant insideDeterminant = new InsideDeterminant(event.getBid(), event.getPid());
                insideDeterminant.setAbort(true);
                targetId = collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false, insideDeterminant,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            } else {
                targetId = collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false, null , event.getTimestamp());//the tuple is finished.//the tuple is abort.
            }
            if (enable_upstreamBackup) {
                this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, event.cloneEvent());
            }
            return false;
        }
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
        if(!isReConstruct){
            EventsHolder.add(event);
            if (enable_recovery_dependency) {
                MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
                String[] keys = new String[]{event.getSourceAccountId(),event.getTargetAccountId(),event.getSourceBookEntryId(), event.getTargetBookEntryId()};
                this.updateRecoveryDependency(keys,true);
                MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
            }
        }
        return true;
    }

    protected void DeterminantDepositRequestConstruct(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEvent.contains(event.getBid())){
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
            Deposit_Request_Construct(event, txnContext, false);
        }
    }

    protected void DeterminantTransferRequestConstruct(TransactionEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        if (event.getBid() < recoveryId) {
            for (CausalService c:this.causalService.values()) {
                if (c.abortEvent.contains(event.getBid())){
                    return;
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
            TransferRequestConstruct(event, txnContext, false);
        }
    }

    protected void AsyncReConstructRequest() throws InterruptedException, DatabaseException {
        Iterator<TxnEvent> it = EventsHolder.iterator();
        while (it.hasNext()) {
            TxnEvent event = it.next();
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, event.getBid());
            if(event instanceof DepositEvent){
                if(!Deposit_Request_Construct((DepositEvent) event,txnContext,true)){
                    it.remove();
                }
            }else{
                if(!TransferRequestConstruct((TransactionEvent) event,txnContext,true)){
                    it.remove();
                }
            }
        }
    }

    protected void CommitOutsideDeterminant(long markId) throws DatabaseException, InterruptedException {
        if ((enable_key_based || this.executor.isFirst_executor()) && !this.causalService.isEmpty()) {
            for (CausalService c:this.causalService.values()) {
                for (OutsideDeterminant outsideDeterminant:c.outsideDeterminant) {
                    if (outsideDeterminant.outSideEvent.getBid() < markId) {
                        TxnContext txnContext = new TxnContext(thread_Id,this.fid,outsideDeterminant.outSideEvent.getBid());
                        if (outsideDeterminant.outSideEvent instanceof DepositEvent) {
                            DeterminantDepositRequestConstruct((DepositEvent) outsideDeterminant.outSideEvent, txnContext);
                        } else {
                            DeterminantTransferRequestConstruct((TransactionEvent) outsideDeterminant.outSideEvent, txnContext);
                        }
                    } else {
                        break;
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
                OutsideDeterminant outsideDeterminant = null;
                if (enable_determinants_log) {
                    MeasureTools.HelpLog_backup_begin(this.thread_Id, System.nanoTime());
                    outsideDeterminant = new OutsideDeterminant();
                    outsideDeterminant.setOutSideEvent(event);
                    String[] keys;
                    if(event instanceof TransactionEvent){
                        keys = new String[]{((TransactionEvent)event).getSourceAccountId(), ((TransactionEvent)event).getSourceAccountId(),
                                ((TransactionEvent)event).getTargetAccountId(), ((TransactionEvent)event).getTargetBookEntryId()};
                    }else{
                        keys = new String[]{((DepositEvent)event).getAccountId(), ((DepositEvent)event).getBookEntryId()};
                    }
                    for (String id : keys) {
                        if (this.getPartitionId(id) != event.getPid()) {
                            outsideDeterminant.setTargetPartitionId(this.getPartitionId(id));
                        }
                    }
                    MeasureTools.HelpLog_backup_acc(this.thread_Id, System.nanoTime());
                }
                int targetId;
                if (outsideDeterminant!=null && !outsideDeterminant.targetPartitionIds.isEmpty()) {
                    targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, outsideDeterminant, event.getTimestamp());//the tuple is finished.
                } else {
                    targetId = collector.emit_single(DEFAULT_STREAM_ID, event.getBid(), true, null, event.getTimestamp());//the tuple is finished.
                }
                if (enable_upstreamBackup) {
                    MeasureTools.Upstream_backup_begin(this.executor.getExecutorID(), System.nanoTime());
                    this.multiStreamInFlightLog.addEvent(targetId - firstDownTask, DEFAULT_STREAM_ID, event.cloneEvent());
                    MeasureTools.Upstream_backup_acc(this.executor.getExecutorID(), System.nanoTime());
                }
            }
            MeasureTools.Upstream_backup_finish_acc(this.executor.getExecutorID());
        }
    }

    @Override
    protected void REQUEST_CORE() throws InterruptedException {
        for (TxnEvent event:EventsHolder){
            if(event instanceof TransactionEvent){
                TRANSFER_REQUEST_CORE((TransactionEvent) event);
            }else {
                DEPOSITE_REQUEST_CORE((DepositEvent) event);
            }
        }
    }

    private void TRANSFER_REQUEST_CORE(TransactionEvent event) throws InterruptedException {
        event.transaction_result = new TransactionResult(event, event.success[0],event.src_account_value.getRecord().getValues().get(1).getLong(), event.src_asset_value.getRecord().getValues().get(1).getLong());
    }
    protected void DEPOSITE_REQUEST_CORE(DepositEvent event) {

    }
}
