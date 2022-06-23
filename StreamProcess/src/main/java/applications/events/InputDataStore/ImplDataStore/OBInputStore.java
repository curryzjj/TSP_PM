package applications.events.InputDataStore.ImplDataStore;

import applications.events.InputDataStore.InputStore;
import applications.events.TxnEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class OBInputStore extends InputStore {
    private static final long serialVersionUID = -7129909756094526547L;

    @Override
    public void storeInput(List<TxnEvent> inputs) throws IOException {
        File file=new File(inputFile.concat(inputStorePaths.get(currentOffset)));
        FileWriter Fw= null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for(TxnEvent event:inputs){
            StringBuilder sb = new StringBuilder();
            if (event instanceof BuyingEvent) {
                sb.append(((BuyingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).num_p());//3
                sb.append(split_exp);
                sb.append("BuyingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getItemId()));//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidPrice()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidQty()));//7
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).getTimestamp());
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).isAbort());
            } else if (event instanceof AlertEvent) {
                sb.append(((AlertEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((AlertEvent) event).num_p());
                sb.append(split_exp);
                sb.append("AlertEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getAsk_price()));
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getTimestamp());
                sb.append(split_exp);
                sb.append(((AlertEvent) event).isAbort());
            } else if (event instanceof ToppingEvent) {
                sb.append(((ToppingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).num_p());
                sb.append(split_exp);
                sb.append("ToppingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemTopUp()));
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getTimestamp());
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).isAbort());
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
