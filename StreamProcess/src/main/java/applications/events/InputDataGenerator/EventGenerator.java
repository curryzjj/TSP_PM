package applications.events.InputDataGenerator;

import System.util.Configuration;
import applications.events.TxnEvent;
import net.openhft.affinity.AffinityLock;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import static UserApplications.CONTROL.Arrival_Control;
import static streamprocess.execution.affinity.SequentialBinding.next_cpu;
import static xerial.jnuma.Numa.setLocalAlloc;

public class EventGenerator extends Thread {
    private final static Logger LOG= LoggerFactory.getLogger(EventGenerator.class);
    private final int loadTargetHz;
    private final int timeSliceLengthMs;
    private final int elements;
    private final int batch;
    private long sleep_time;
    private long busy_time;
    private long cnt=0;
    private long startTime;
    private long finishTime;
    private long busyTime=0;
    private int spoutThreads;
    private CountDownLatch latch;
    //<ExecutorId, inputQueue>
    public HashMap<Integer, Queue<TxnEvent>> EventsQueues = new HashMap<>();
    public Queue<List<TxnEvent>> EventsQueue;
    private final long exe;
    private Configuration configuration;
    public EventGenerator(Configuration configuration, CountDownLatch latch){
        this.configuration = configuration;
        this.loadTargetHz = configuration.getInt("targetHz");
        this.timeSliceLengthMs = configuration.getInt("timeSliceLengthMs");
        this.elements = this.loadPerTimeslice();
        this.EventsQueue = new MpscArrayQueue<>(20000000);
        this.batch = configuration.getInt("input_store_batch");
        spoutThreads = configuration.getInt("spoutThread", 1);//now read from parameters.
        this.exe = configuration.getInt("NUM_EVENTS");
        this.latch = latch;
        for (int i = 0; i < spoutThreads; i++) {
            EventsQueues.put(i, new MpscArrayQueue<>((int) (exe/spoutThreads)));
        }
    }

    @Override
    public void run() {
        boolean finish = false;
        sequential_binding();
        latch.countDown();//tells others I'm really ready.
        try {
            latch.await();//wait all the thread to be ready
        }catch(InterruptedException ignored){

        }
        this.startTime = System.nanoTime();
        while (!finish){
          if(Arrival_Control){
              finish = seedDataWithControl();
              cnt = cnt + elements;
          }else{
              finish = seedDataWithoutControl();
          }
        }
        LOG.info("Event arrival rate is "+ exe * 1E6 / (finishTime - startTime) + " (k input_event/s)" + "busy time: " + busy_time + " sleep time: " + sleep_time + " cnt " + cnt);
    }
    private boolean seedDataWithControl(){
        long emitStartTime = System.currentTimeMillis();
        int circle = elements / batch;
        while (circle != 0){
            List<TxnEvent> events = DataHolder.ArrivalData(batch);
            if(events.size() == batch){
                this.EventsQueue.offer(events);
                circle --;
            }else{
                this.EventsQueue.offer(events);
                this.EventsQueue.offer(new ArrayList<>());
                this.finishTime = System.nanoTime();
                return true;
            }
        }
        // Sleep for the rest of time slice if needed
        long finishTime = System.currentTimeMillis();
        long emitTime = finishTime - emitStartTime;
        if (emitTime < timeSliceLengthMs) {
            try {
                Thread.sleep(timeSliceLengthMs - emitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sleep_time ++;
        } else{
            this.busyTime = busyTime + emitTime - timeSliceLengthMs;
            busy_time ++;
        }
        return false;
    }
    private boolean seedDataWithoutControl(){
        boolean finish=false;
        while (!finish){
            List<TxnEvent> events = DataHolder.ArrivalData(batch);
            if(events != null){
                this.EventsQueue.offer(events);
                cnt=cnt+batch;
            }else{
                events = new ArrayList<>();
                this.EventsQueue.offer(events);
                this.finishTime = System.nanoTime();
                finish = true;
            }
        }
        return finish;
    }
    private int loadPerTimeslice(){
        return loadTargetHz * timeSliceLengthMs/1000;//make each spout thread independent
    }

    public Queue<List<TxnEvent>> getEventsQueue() {
        return EventsQueue;
    }
    //bind Thread
    protected void sequential_binding(){
        setLocalAlloc();
        int cpu = next_cpu();
        AffinityLock.acquireLock(cpu);
        LOG.info( "Event Generator binding to cpu:"+ cpu);
    }
}
