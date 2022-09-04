package applications.events.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.FastZipfGenerator;
import System.tools.randomNumberGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.SL.DepositEvent;
import applications.events.SL.SLParam;
import applications.events.SL.TransactionEvent;
import applications.events.TxnEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static UserApplications.CONTROL.*;
import static UserApplications.CONTROL.RATIO_OF_READ;
import static UserApplications.constants.StreamLedgerConstants.Constant.*;

public class SLDataGenerator extends InputDataGenerator {
    private final static Logger LOG = LoggerFactory.getLogger(SLDataGenerator.class);
    private static final long serialVersionUID = -2917472754514204127L;

    @Override
    public List<AbstractInputTuple> generateData(int batch) {
        return null;
    }

    @Override
    public List<TxnEvent> generateEvent(int batch) {
        List<TxnEvent> batch_event=new ArrayList<>();
        if(recordNum==0){
            return null;
        }
        for(int i=0;i<Math.min(recordNum,batch);i++){
            TxnEvent event= (TxnEvent) this.create_new_event(current_bid);
            batch_event.add(event);
        }
        recordNum=recordNum-Math.min(recordNum,batch);
        return batch_event;
    }

    @Override
    public void generateEvent() {
        for(int i = 0; i < recordNum; i++){
            TxnEvent event= (TxnEvent) this.create_new_event(current_bid);
            events.add(event);
        }
    }

    @Override
    public void storeInput(Object input) throws IOException {
        List<TxnEvent> inputs= (List<TxnEvent>) input;
        File file=new File(dataPath);
        FileWriter Fw= null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for(TxnEvent event:inputs){
            StringBuilder sb = new StringBuilder();
            if (event instanceof DepositEvent) {
                sb.append(((DepositEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((DepositEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((DepositEvent) event).num_p());////3 num of p
                sb.append(split_exp);
                sb.append("DepositEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getAccountId());//5
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getBookEntryId());//6
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getAccountTransfer());//7
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getBookEntryTransfer());//8
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getTimestamp());
                sb.append(split_exp);
                sb.append(((DepositEvent) event).isAbort());
            } else if (event instanceof TransactionEvent) {
                sb.append(((TransactionEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getPid());//1 -- pid.
                sb.append(split_exp);
                sb.append(Arrays.toString(((TransactionEvent) event).getBid_array()));//2 -- bid array
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).num_p());//3 num of p
                sb.append(split_exp);
                sb.append("TransactionEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getSourceAccountId());//5
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getSourceBookEntryId());//6
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getTargetAccountId());//7
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getTargetBookEntryId());//8
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getAccountTransfer());//9
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getBookEntryTransfer());//10
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getTimestamp());
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).isAbort());
            }
            bw.write(sb.toString() + "\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public void initialize(String dataPath, Configuration config) {
        super.initialize(dataPath,config);
    }

    @Override
    public Object create_new_event(int bid) {
        boolean flag = next_decision();
        if(flag){
            return randomDepositEvent(p_bid,bid);
        }else {
            return randomTransferEvent(p_bid,bid);
        }
    }
    @Override
    public void close() {
        LocalFileSystem fs=new LocalFileSystem();
        try {
            fs.delete(new Path(dataPath),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private Object randomDepositEvent(long[] bid_array,long bid){
        SLParam param = new SLParam(2);
        Set<Integer> keys = new HashSet<>();
        current_pid = key_to_partition(partitionId_generator.next());
        randomKeys(param,keys,2);
        final int account = param.keys()[0];
        final int book = param.keys()[1];
        final long accountsDeposit = rnd.nextLong(MAX_ACCOUNT_DEPOSIT) + 500;
        final long bookDeposit= rnd.nextLong(MAX_BOOK_DEPOSIT) + 500;
        current_bid++;
        if (random.nextInt(1000) < RATIO_OF_ABORT) {
            return new DepositEvent(bid, current_pid, bid_array, partition_num, String.valueOf(account), String.valueOf(book), MIN_BALANCE, MIN_BALANCE, true);
        } else {
            return new DepositEvent(bid, current_pid, bid_array, partition_num, String.valueOf(account), String.valueOf(book), accountsDeposit, bookDeposit, false);
        }

    }
    private Object randomTransferEvent(long[] bid_array,long bid){
        SLParam param = new SLParam(4);
        Set<Integer> keys = new HashSet<>();
        current_pid = key_to_partition(partitionId_generator.next());
        randomKeys(param,keys,4);
        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);
        final int sourceAcct = param.keys()[0];
        final int targetAcct = param.keys()[1];
        final int sourceBook = param.keys()[2];
        final int targetBook = param.keys()[3];
        current_bid++;
        if (random.nextInt(1000) < RATIO_OF_ABORT) {
            return new TransactionEvent(bid, current_pid, bid_array, partition_num, String.valueOf(sourceAcct),  String.valueOf(sourceBook), String.valueOf(targetAcct), String.valueOf(targetBook), accountsTransfer, transfer, MIN_BALANCE,true);
        } else {
            return new TransactionEvent(bid, current_pid, bid_array, partition_num, String.valueOf(sourceAcct),  String.valueOf(sourceBook), String.valueOf(targetAcct), String.valueOf(targetBook), accountsTransfer, transfer, MAX_BALANCE,false);
        }
    }
}
