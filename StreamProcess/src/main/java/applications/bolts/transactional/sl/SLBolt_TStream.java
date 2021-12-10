package applications.bolts.transactional.sl;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.SL.TransactionResult;
import applications.events.TxnEvent;
import engine.Exception.DatabaseException;
import engine.table.datatype.DataBox;
import engine.transaction.TxnContext;
import engine.transaction.function.Condition;
import engine.transaction.function.DEC;
import engine.transaction.function.INC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;

public abstract class SLBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(SLBolt_TStream.class);
    List<TxnEvent> EventsHolder=new ArrayList<>();
    public SLBolt_TStream(int fid) {
        super(LOG, fid);
        this.configPrefix="tpsl";
        status=new Status();
        this.setStateful();
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id, this.fid,in.getBID());
        TxnEvent event = (TxnEvent) in.getValue(0);
        if (event instanceof DepositEvent) {
            DEPOSITE_REQUEST_CONSTRUCT((DepositEvent) event, txnContext,false);
        } else {
            TRANSFER_REQUEST_CONSTRUCT((TransactionEvent) event, txnContext,false);
        }
    }
    public void BUFFER_PROCESS() throws DatabaseException, InterruptedException {
        if(bufferedTuple.isEmpty()){
        }else{
            for (Tuple tuple : bufferedTuple) {
                PRE_TXN_PROCESS(tuple);
            }
        }
    }
    protected boolean DEPOSITE_REQUEST_CONSTRUCT(DepositEvent event, TxnContext txnContext,boolean isReConstruct) throws DatabaseException, InterruptedException {
        boolean flag=transactionManager.Asy_ModifyRecord(txnContext,"accounts",event.getAccountId(),new INC(event.getAccountTransfer()));
        if(!flag){
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            return flag;
        }
        transactionManager.Asy_ModifyRecord(txnContext,"bookEntries",event.getBookEntryId(),new INC(event.getBookEntryTransfer()));
        if(!isReConstruct){
            EventsHolder.add(event);
        }
        return flag;
    }
    protected boolean TRANSFER_REQUEST_CONSTRUCT(TransactionEvent event, TxnContext txnContext,boolean isReConstruct) throws DatabaseException, InterruptedException {
        String[] srcTable = new String[]{"accounts", "bookEntries"};
        String[] srcID = new String[]{event.getSourceAccountId(), event.getSourceBookEntryId()};
        boolean flag=transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getSourceAccountId()
                , event.src_account_value,//to be fill up.
                new DEC(event.getAccountTransfer()),
                srcTable, srcID,//condition source, condition id.
                new Condition(
                        event.getMinAccountBalance(),
                        event.getAccountTransfer(),
                        event.getBookEntryTransfer()),
                event.success);
        if(!flag){
            collector.emit_single(DEFAULT_STREAM_ID,event.getBid(), false,event.getTimestamp());//the tuple is finished.//the tuple is abort.
            return flag;
        }
        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries", event.getSourceBookEntryId()
                , new DEC(event.getBookEntryTransfer()), srcTable, srcID,
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "accounts",
                event.getTargetAccountId()
                , event.dst_account_value,//to be fill up.
                new INC(event.getAccountTransfer()),
                srcTable, srcID//condition source, condition id.
                , new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);          //asynchronously return.

        transactionManager.Asy_ModifyRecord(txnContext,
                "bookEntries",
                event.getTargetBookEntryId()
                , new INC(event.getBookEntryTransfer()), srcTable, srcID,
                new Condition(event.getMinAccountBalance(),
                        event.getAccountTransfer(), event.getBookEntryTransfer()),
                event.success);   //asynchronously return.
        if(!isReConstruct){
            EventsHolder.add(event);
        }
        return flag;
    }
    protected void AsyncReConstructRequest() throws InterruptedException, DatabaseException {
        Iterator<TxnEvent> it=EventsHolder.iterator();
        while (it.hasNext()) {
            TxnEvent event = it.next();
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, event.getBid());
            if(event instanceof DepositEvent){
                if(!DEPOSITE_REQUEST_CONSTRUCT((DepositEvent) event,txnContext,true)){
                    it.remove();
                }
            }else{
                if(!TRANSFER_REQUEST_CONSTRUCT((TransactionEvent) event,txnContext,true)){
                    it.remove();
                }
            }
        }
    }

    @Override
    protected void REQUEST_POST() throws InterruptedException {
        for (TxnEvent event:EventsHolder){
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        }
    }

    @Override
    protected void REQUEST_REQUEST_CORE() throws InterruptedException {
        for (TxnEvent event:EventsHolder){
            if(event instanceof TransactionEvent){
                TRANSFER_REQUEST_CORE((TransactionEvent) event);
            }else {
                DEPOSITE_REQUEST_CORE((DepositEvent) event);
            }
        }
    }

    private void TRANSFER_REQUEST_CORE(TransactionEvent event) throws InterruptedException {
        event.transaction_result = new TransactionResult(event, event.success[0],event.src_account_value.getRecord().getValues().get(1).getLong(), event.dst_account_value.getRecord().getValues().get(1).getLong());
    }
    protected void DEPOSITE_REQUEST_CORE(DepositEvent event) {
        List<DataBox> values = event.account_value.getRecord().getValues();
        long newAccountValue = values.get(1).getLong() + event.getAccountTransfer();
        values.get(1).setLong(newAccountValue);
        List<DataBox> asset_values = event.asset_value.getRecord().getValues();
        long newAssetValue = values.get(1).getLong() + event.getBookEntryTransfer();
        asset_values.get(1).setLong(newAssetValue);
    }
}
