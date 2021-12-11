package applications.events.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.FastZipfGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.SL.DepositEvent;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static UserApplications.CONTROL.*;
import static UserApplications.CONTROL.RATIO_OF_READ;
import static UserApplications.constants.StreamLedgerConstants.Constant.*;

public class SLDataGenerator extends InputDataGenerator {
    private final static Logger LOG= LoggerFactory.getLogger(SLDataGenerator.class);
    protected int[] event_decision_distribute;
    protected int event_decision_id;
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
        if(enable_snapshot||enable_wal){
            try {
                storeInput(batch_event);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        recordNum=recordNum-Math.min(recordNum,batch);
        return batch_event;
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
            }
            bw.write(sb.toString() + "\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public void initialize(String dataPath, int recordNum, int range, double zipSkew, Configuration config) {
        this.recordNum=recordNum;
        this.dataPath=dataPath;
        this.zipSkew=zipSkew;
        this.range=range;
        this.current_pid=0;
        this.event_decision_id=0;
        this.partition_num =config.getInt("partition_num");
        if(enable_states_partition){
            floor_interval= (int) Math.floor(NUM_ITEMS / (double) partition_num);//NUM_ITEMS / partition_num;
            partitioned_store =new FastZipfGenerator[partition_num];
            for (int i = 0; i < partition_num; i++) {
                partitioned_store[i] = new FastZipfGenerator((int) floor_interval, zipSkew, i * floor_interval);
            }
        }else{
            this.shared_store=new FastZipfGenerator(NUM_ITEMS, zipSkew,0);
        }
        p_bid = new long[partition_num];

        for (int i = 0; i < partition_num; i++) {
            p_bid[i] = 0;
        }
        if (RATIO_OF_READ == 0) {
            this.event_decision_distribute=new int[]{1,1,1,1,1,1,1,1};//Transfer
        } else if (RATIO_OF_READ == 0.25) {
            this.event_decision_distribute=new int[]{0,0,1,1,1,1,1,1};//Deposit:Transfer
        } else if (RATIO_OF_READ == 0.5) {
            this.event_decision_distribute=new int[]{0,0,0,0,1,2,1,2};//
        } else if (RATIO_OF_READ == 0.75) {
            this.event_decision_distribute=new int[]{0,0,0,0,0,0,1,2};//
        } else if (RATIO_OF_READ == 1) {
            this.event_decision_distribute=new int[]{0,0,0,0,0,0,0,0};//Deposit
        } else {
            throw new UnsupportedOperationException();
        }
        LOG.info("ratio_of_read: " + RATIO_OF_READ + "\tREAD DECISIONS: " + Arrays.toString(read_decision));
    }

    @Override
    public Object create_new_event(int bid) {
        int flag=next_decision();
        if(flag==0){
            return randomDepositEvent(p_bid,bid);
        }else {
            return randomTransferEvent(p_bid,bid);
        }
    }
    protected int next_decision() {
        int rt = event_decision_distribute[event_decision_id];
        event_decision_id++;
        if (event_decision_id == 8)
            event_decision_id = 0;
        return rt;
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
        final int account;//key
        if (enable_states_partition)
            account = partitioned_store[current_pid].next();//rnd.nextInt(account_range) + partition_offset;
        else
            account = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
        if (partition_num > 1) {//multi-partition
            current_pid++;
            if (current_pid == partition_num)
                current_pid = 0;
        }
        final int book;
        if (enable_states_partition)
            book = partitioned_store[current_pid].next();//rnd.nextInt(asset_range) + partition_offset;
        else
            book = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
        if (partition_num > 1) {//multi-partition
            current_pid++;
            if (current_pid == partition_num)
                current_pid = 0;
        }
        final long accountsDeposit= rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long deposit= rnd.nextLong(MAX_BOOK_TRANSFER);
        current_bid++;
        return new DepositEvent(bid,
                current_pid,
                bid_array,
                partition_num,
                ACCOUNT_ID_PREFIX+account,
                BOOK_ENTRY_ID_PREFIX+book,
                accountsDeposit,
                deposit);
    }
    private Object randomTransferEvent(long[] bid_array,long bid){
        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);
        while(true){
            final int sourceAcct;
            if (enable_states_partition)
                sourceAcct = partitioned_store[current_pid].next();//rnd.nextInt(account_range) + partition_offset;
            else
                sourceAcct = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
            if (partition_num > 1) {//multi-partition
                current_pid++;
                if (current_pid == partition_num)
                    current_pid = 0;
            }
            final int targetAcct;
            if (enable_states_partition)
                targetAcct = partitioned_store[current_pid].next();//rnd.nextInt(account_range) + partition_offset;
            else
                targetAcct = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
            if (partition_num > 1) {//multi-partition
                current_pid++;
                if (current_pid == partition_num)
                    current_pid = 0;
            }
            final int sourceBook;
            if (enable_states_partition)
                sourceBook = partitioned_store[current_bid].next();//rnd.nextInt(asset_range) + partition_offset;
            else
                sourceBook = shared_store.next();//rnd.nextInt(account_range) + partition_offset;

            if (partition_num > 1) {//multi-partition
                current_bid++;
                if (current_bid == partition_num)
                    current_bid = 0;
            }
            final int targetBook;
            if (enable_states_partition)
                targetBook = partitioned_store[current_pid].next();//rnd.nextInt(asset_range) + partition_offset;
            else
                targetBook = shared_store.next();//rnd.nextInt(asset_range) + partition_offset;

            if (sourceAcct == targetAcct || sourceBook == targetBook) {
                continue;
            }
            current_bid++;
            return new TransactionEvent(
                    bid,
                    current_pid,
                    bid_array,
                    partition_num,
                    ACCOUNT_ID_PREFIX + sourceAcct,
                    BOOK_ENTRY_ID_PREFIX + sourceBook,
                    ACCOUNT_ID_PREFIX + targetAcct,
                    BOOK_ENTRY_ID_PREFIX + targetBook,
                    accountsTransfer,
                    transfer,
                    MIN_BALANCE);
        }
    }
}
