package applications.events.InputDataGenerator;

import applications.events.TxnEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class DataHolder {
    public static Queue<TxnEvent> events = new ArrayDeque<>();
    public static List<TxnEvent> ArrivalData(int batch) {
        List<TxnEvent> arrivalData = new ArrayList<>();
        while(batch != 0 && events.size() !=0) {
            TxnEvent txnEvent = events.poll();
            txnEvent.UpdateTimestamp(System.nanoTime());
            arrivalData.add(txnEvent);
            batch --;
        }
        return arrivalData;
    }
}
