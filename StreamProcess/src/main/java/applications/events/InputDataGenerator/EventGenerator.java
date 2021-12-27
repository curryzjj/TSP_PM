package applications.events.InputDataGenerator;

import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.InputDataGenerator.ImplDataGenerator.TPDataGenerator;
import applications.events.TxnEvent;
import net.openhft.affinity.AffinityLock;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static UserApplications.CONTROL.Arrival_Control;
import static streamprocess.execution.affinity.SequentialBinding.next_cpu;
import static xerial.jnuma.Numa.setLocalAlloc;

public class EventGenerator extends Thread {
    private final static Logger LOG= LoggerFactory.getLogger(EventGenerator.class);
    private InputDataGenerator inputDataGenerator;
    private final int loadTargetHz;
    private final int timeSliceLengthMs;
    private final int elements;
    private final int batch;
    private long sleep_time;
    private long busy_time;
    private long cnt=0;
    private long startTime;
    public Queue EventsQueue;
    private boolean isReport=false;
    public void setInputDataGenerator(InputDataGenerator inputDataGenerator) {
        this.inputDataGenerator = inputDataGenerator;
        if(inputDataGenerator instanceof TPDataGenerator){
            isReport=true;
        }
    }
    private Configuration configuration;
    public EventGenerator(Configuration configuration){
        this.configuration=configuration;
        this.loadTargetHz=configuration.getInt("targetHz");
        this.timeSliceLengthMs=configuration.getInt("timeSliceLengthMs");
        this.elements=this.loadPerTimeslice();
        this.EventsQueue=new MpscArrayQueue(20000000);
        this.batch=configuration.getInt("input_store_batch");
    }

    @Override
    public void run() {
        boolean finish=false;
        sequential_binding();
        this.startTime=System.nanoTime();
        while (!finish){
          if(Arrival_Control){
              finish=seedDataWithControl();
              cnt=cnt+elements;
          }else{
              finish=seedDataWithoutControl();
          }
        }
        LOG.info("Event arrival rate is "+ cnt*1E6/(System.nanoTime()-startTime)+" (k input_event/s)" +"busy time: "+busy_time+" sleep time: "+sleep_time+" cnt "+cnt);
    }
    private boolean seedDataWithControl(){
        boolean finish=false;
        long emitStartTime = System.currentTimeMillis();
        int circle=elements/batch;
        if(isReport){
            while (circle!=0){
                List<AbstractInputTuple> tuples=this.inputDataGenerator.generateData(batch);
                if(tuples!=null){
                    this.EventsQueue.offer(tuples);
                    circle--;
                }else{
                    tuples=new ArrayList<>();
                    this.EventsQueue.offer(tuples);
                    finish=true;
                    circle=0;
                }
            }
        }else{
            while (circle!=0){
                List<TxnEvent> events=this.inputDataGenerator.generateEvent(batch);
                if(events!=null){
                    this.EventsQueue.offer(events);
                    circle--;
                }else{
                    events=new ArrayList<>();
                    this.EventsQueue.offer(events);
                    finish=true;
                    circle=0;
                    return finish;
                }
            }
        }
        // Sleep for the rest of timeslice if needed
        long emitTime = System.currentTimeMillis() - emitStartTime;
        if (emitTime < timeSliceLengthMs) {// in terms of milliseconds.
            try {
                Thread.sleep(timeSliceLengthMs - emitTime);
            } catch (InterruptedException ignored) {
                //  e.printStackTrace();
            }
            sleep_time++;
        } else
            busy_time++;
        return finish;
    }
    private boolean seedDataWithoutControl(){
        boolean finish=false;
        if(isReport){
            while (!finish){
                List<AbstractInputTuple> tuples=this.inputDataGenerator.generateData(batch);
                if(tuples!=null){
                    this.EventsQueue.offer(tuples);
                    cnt=cnt+batch;
                }else{
                    tuples=new ArrayList<>();
                    this.EventsQueue.offer(tuples);
                    finish=true;
                }
            }
        }else{
            while (!finish){
                List<TxnEvent> events=this.inputDataGenerator.generateEvent(batch);
                if(events!=null){
                    this.EventsQueue.offer(events);
                    cnt=cnt+batch;
                }else{
                    events=new ArrayList<>();
                    this.EventsQueue.offer(events);
                    finish=true;
                }
            }
        }
        return finish;
    }
    private int loadPerTimeslice(){
        return loadTargetHz/(1000/timeSliceLengthMs);//make each spout thread independent
    }

    public Queue getEventsQueue() {
        return EventsQueue;
    }
    //bind Thread
    protected long[] sequential_binding(){
        setLocalAlloc();
        int cpu = next_cpu();
        AffinityLock.acquireLock(cpu);
        LOG.info( "Event Generator binding to cpu:"+ cpu);
        return null;
    }
}
