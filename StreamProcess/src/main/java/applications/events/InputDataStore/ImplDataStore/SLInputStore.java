package applications.events.InputDataStore.ImplDataStore;

import applications.events.InputDataStore.InputStore;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.TxnEvent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SLInputStore extends InputStore {
    private static final long serialVersionUID = -8628107878806536242L;

    @Override
    public void storeInput(List<TxnEvent> inputs) throws IOException {
        File file=new File(inputFile.concat(inputStorePaths.get(currentOffset)));
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
    public void close() {

    }
}
