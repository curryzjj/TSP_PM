package applications.events;

import java.util.concurrent.ConcurrentSkipListSet;

public class GlobalSorter {
    public static ConcurrentSkipListSet<TxnEvent> sortedEvents = new ConcurrentSkipListSet<>();

    public static void addEvent(TxnEvent event) {
        sortedEvents.add(event);
    }
}
