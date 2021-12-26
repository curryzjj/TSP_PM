package streamprocess.execution.runtime.threads;

import System.util.Configuration;
import ch.usi.overseer.OverHpc;
import engine.Clock;
import engine.Exception.DatabaseException;
import net.openhft.affinity.AffinitySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import streamprocess.components.operators.executor.BasicSpoutBatchExecutor;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static UserApplications.CONTROL.*;
import static net.openhft.affinity.AffinityLock.dumpLocks;

public class spoutThread extends executorThread{
    private static final Logger LOG = LoggerFactory.getLogger(spoutThread.class);
    private final BasicSpoutBatchExecutor sp;
    private final int loadTargetHz;
    private final int timeSliceLengthMs;
    private final int elements;
    private final OutputCollector collector;
    boolean binding_finish=false;
    int sleep_time = 0;
    int busy_time = 0;


    int _combo_bid_size = 1;

    public spoutThread(ExecutionNode e,TopologyContext context , Configuration conf, long[] cpu,
                       int node, CountDownLatch latch,int loadTargetHz, int timeSliceLengthMs, OverHpc HPCMonotor,
                       HashMap<Integer, executorThread> threadMap, Clock clock) {
        super(e, conf, context, cpu, node, latch, HPCMonotor, threadMap);
        this.sp=(BasicSpoutBatchExecutor) e.op;
        this.collector = new OutputCollector(e,context);
        batch = conf.getInt("input_store_batch", 1);
        this.loadTargetHz = loadTargetHz;
        this.timeSliceLengthMs = timeSliceLengthMs;
        sp.setExecutionNode(e);
        sp.setclock(clock);
        elements = loadPerTimeslice();//how many elements are required to sent each time
        //switch the control methord
    }

    @Override
    protected void _execute_noControl() throws InterruptedException, IOException {
        sp.bulk_emit(batch);
        cnt=cnt+batch;
    }

    /**
     * Only spout need to control
     */
    protected void _execute_withControl() throws InterruptedException, IOException {
        long emitStartTime = System.currentTimeMillis();
        sp.bulk_emit(elements);
        cnt += elements;
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
    }
    @Override
    protected void _execute() throws InterruptedException, IOException {
        _execute_noControl();
    }

    @Override
    protected void _profile() throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        //Some conditions
        sp.bulk_emit(batch);
    }
    @Override
    public void run() {
        try{
            Thread.currentThread().setName("Operator:"+executor.getOP()+"\tExecutor ID:"+executor.getExecutorID());
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            initilize_queue(executor.getExecutorID());
            sp.prepare(conf,context,collector);
            long[] binding=null;
            if (!conf.getBoolean("NAV",true)){
                binding=binding();
            }
            if(enable_numa_placement){
                if(conf.getBoolean("Sequential_Binding",true)){
                 binding=sequential_binding();
                }
            }
            if(binding!=null){
                LOG.info("Successfully create spoutExecutors "+sp.getContext().getThisTaskId()+"on node:"+
                        ""+node+"binding:"+Long.toBinaryString(0x1000000000000000L| binding[0]).substring(1));
            }
            binding_finish=true;
            if (enable_shared_state)
                LOG.info("Operator:\t"+executor.getOP_full()+"is ready"+"\nlock_ratio dumps\n"+dumpLocks());
            this.Ready(LOG);
            System.gc();
            latch.countDown();//tells others I'm really ready.
            try {
                latch.await();//wait all the thread to be ready
            }catch(InterruptedException ignored){

            }
            if(this.executor.needsProfile()){
                profile_routing(context.getGraph().topology.getPlatform());
            }else {
                LOG.info(this.executor.getOP_full()+" started");
                routing();
            }
        } catch (DatabaseException | BrokenBarrierException | InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            if (lock!=null){
                lock.release();
            }
            this.executor.display();
            if(end_emit==0){//Interrupt
                end_emit=System.nanoTime();
            }
            double actual_throughput=(cnt-this.executor.op.getEmpty())*1E6/(end_emit-start_emit);//k event per second
            if(TopologyContext.plan.getSP()!=null){
                //some function to calculate the expected_throughput
            } else{
                expected_throughput=actual_throughput;
            }
            LOG.info(this.executor.getOP_full()
                            + "\tfinished execution and exit with throughput (k input_event/s) of:\t"
                            + actual_throughput+ "(" + actual_throughput / expected_throughput + ")"
                            + " on node: " + node+" cnt="+cnt
            );
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                //e.printStackTrace();
            }
        }
    }
    private int loadPerTimeslice(){
        return loadTargetHz/(1000/timeSliceLengthMs);//make each spout thread independent
    }
}
