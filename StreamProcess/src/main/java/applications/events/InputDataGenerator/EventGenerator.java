package applications.events.InputDataGenerator;

import System.util.Configuration;
import System.util.OsUtils;
import applications.DataTypes.AbstractInputTuple;
import applications.events.InputDataGenerator.ImplDataGenerator.TPDataGenerator;
import applications.events.TxnEvent;
import net.openhft.affinity.AffinityLock;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.LinkedQueue;

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
    private long finishTime;
    private long lastFinishTime;
    public Queue EventsQueue;
    private long exe;
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
        if(OsUtils.isMac()){
            this.exe= configuration.getInt("TEST_NUM_EVENTS");
        }else{
            this.exe=configuration.getInt("NUM_EVENTS");
        }
    }

    @Override
    public void run() {
        boolean finish=false;
        sequential_binding();
        this.startTime=System.nanoTime();
        this.lastFinishTime=System.currentTimeMillis();
        while (!finish){
          if(Arrival_Control){
              finish=seedDataWithControl();
              cnt=cnt+elements;
          }else{
              finish=seedDataWithoutControl();
          }
        }
        LOG.info("Event arrival rate is "+ exe*1E6/(finishTime-startTime)+" (k input_event/s)" +"busy time: "+busy_time+" sleep time: "+sleep_time+" cnt "+cnt);
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
                    this.finishTime=System.nanoTime();
                    return finish;
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
                    this.finishTime=System.nanoTime();
                    return finish;
                }
            }
        }
        // Sleep for the rest of timeslice if needed
        long finishTime=System.currentTimeMillis();
        long emitTime = finishTime - emitStartTime;
        if (emitTime < timeSliceLengthMs) {// in terms of milliseconds.
            try {
                if(timeSliceLengthMs-emitTime>emitStartTime-lastFinishTime){
                    Thread.sleep(timeSliceLengthMs - emitTime-emitStartTime+lastFinishTime);
                }
            } catch (InterruptedException ignored) {
                //  e.printStackTrace();
            }
            lastFinishTime=System.currentTimeMillis();
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
        return loadTargetHz*timeSliceLengthMs/1000;//make each spout thread independent
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
