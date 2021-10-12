package streamprocess.execution.runtime.collector;

import java.util.concurrent.atomic.AtomicInteger;

public class BIDGenerator {
    private static BIDGenerator ourInstance=new BIDGenerator();
    private final AtomicInteger bid=new AtomicInteger();
    private BIDGenerator(){
        bid.set(0);
    }
    public static BIDGenerator getInstance(){return ourInstance;}
    public int getAndIncrement(){ return bid.getAndIncrement();}
}
