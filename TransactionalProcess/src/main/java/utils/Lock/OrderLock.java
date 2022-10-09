package utils.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SpinLock;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

public class OrderLock implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(OrderLock.class);
    private static final long serialVersionUID = 1347267778748318967L;
    private static OrderLock ourInstance = new OrderLock();

    //	SpinLock spinlock_ = new SpinLock();
//	volatile int fid = 0;
    AtomicLong counter = new AtomicLong(0);// it is already volatiled.
    //	private transient HashMap<Integer, HashMap<Integer, Boolean>> executors_ready;//<FID, ExecutorID, true/false>
    private int end_fid;

    SpinLock check_lock = new SpinLock();

    boolean wasSignalled = false;//to fight with missing signals.

    private OrderLock() {

    }

    public static OrderLock getInstance() {
        return ourInstance;
    }

    public long getBID() {
        return counter.get();
    }
    public void setBID(long bid) {
        this.counter.set(bid);
    }

    protected void fill_gap(LinkedList<Long> gap) {
        for (int i = 0; i < gap.size(); i++) {
            Long g = gap.get(i);
            if (!try_fill_gap(g)) {
                return;
            }
        }
    }

    /**
     * fill the gap.
     *
     * @param g the gap immediately follows previous item.
     */
    public boolean try_fill_gap(Long g) {
        if (getBID() == g) {
            counter.incrementAndGet();//allow next batch to proceed.
            return true;
        }
        return false;
    }


    public boolean blocking_wait(final long bid) throws InterruptedException {
        while (!this.counter.compareAndSet(bid, bid)) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
//                return false;
            }
        }
        return true;
    }

    public void advance() {
        long value = counter.incrementAndGet();//allow next batch to proceed.
    }
}