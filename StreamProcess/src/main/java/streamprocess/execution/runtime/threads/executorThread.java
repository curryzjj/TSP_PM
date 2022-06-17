package streamprocess.execution.runtime.threads;

import System.Platform.Platform;
import System.util.Configuration;
import engine.Exception.DatabaseException;
import net.openhft.affinity.AffinityLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;
import ch.usi.overseer.OverHpc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static streamprocess.execution.affinity.SequentialBinding.next_cpu;
import static xerial.jnuma.Numa.*;

public abstract class executorThread extends Thread {
    private static final Logger LOG= LoggerFactory.getLogger(executorThread.class);
    public final ExecutionNode executor;
    protected final CountDownLatch latch;
    final Configuration conf;
    private final HashMap<Integer, executorThread> threadMap;
    private final OverHpc hpcMonotor;
    public boolean running = true;
    public boolean profiling = false;
    public long[] cpu;
    public int node;
    public boolean migrating = false;
    protected AffinityLock lock;
    double expected_throughput = 0;
    boolean not_yet_profiled = true;
    TopologyContext context;//every thread owns its unique context, which will be pushed to its emitting tuple.
    //for calculating throughput
    double cnt = 0;
    long start_emit = 0;
    long end_emit = 0;
    int batch;
    private boolean start = true;
    private volatile boolean ready = false;
    protected executorThread(ExecutionNode e, Configuration conf, TopologyContext context,
                             long[] cpu, int node, CountDownLatch latch, OverHpc HPCMonotor,
                             HashMap<Integer, executorThread> threadMap){
        this.context=context;
        executor = e;
        this.conf=conf;
        this.cpu=cpu;
        this.node=node;
        this.latch=latch;
        hpcMonotor=HPCMonotor;
        this.threadMap=threadMap;
        if (executor != null && !this.executor.isLeafNode()) {
            this.executor.getController().setContext(this.executor.getExecutorID(), context);
        }

    }
    //get+set TopologyContext
    public TopologyContext getContext() {
        return context;
    }
    public void setContext(TopologyContext context) {
        this.context = context;
    }
    //end

    //bind Thread
    protected long[] sequential_binding(){
          setLocalAlloc();
          int cpu = next_cpu();
          AffinityLock.acquireLock(cpu);
          LOG.info(this.executor.getOP_full() + " binding to node:" + node + " cpu:" + cpu);
          return null;
    }
    protected long[] binding(){
        setLocalAlloc();
        if(executor!=null){
            LOG.info(this.executor.getOP_full());
        }
        LOG.info(" binding to node:" + node + " cpu:" + (int) cpu[0]);
        lock = AffinityLock.acquireLock((int) cpu[0]);
        return getAffinity();
    }
    protected long[] rebinding(){ return null;}
    protected long[] rebinding_clean(){ return null;}
    //end

    //input+output queue
    public void initilize_queue(int executorID) {
        allocate_OutputQueue();
        assign_InputQueue();
    }
    private void allocate_OutputQueue() {
        executor.allocate_OutputQueue(conf.getBoolean("linked", false), (int) (conf.getInt("targetHz") * conf.getInt("snapshot")));
    }
    private void assign_InputQueue(){
        for (String streamId : executor.operator.getOutput_streamsIds()) {
            assign_InputQueue(streamId);
        }
    }
    private void assign_InputQueue(String streamId) {
        executor.setReceive_queueOfChildren(streamId);
    }
    //end

    //pause, restart, and migrate
    private void pause(){}
    private void pause_parent(){}
    private void restart(){}
    private void restart_parent(){}
    //The above is called by _migrate()
    private void _migrate(){}//called by the profile_routing
    public void migrate(){}
    //end

    //profile and routing(Exception->DatabaseException)
    protected abstract void _execute_noControl() throws InterruptedException, DatabaseException, BrokenBarrierException, IOException;
    protected abstract void _execute() throws InterruptedException, DatabaseException, BrokenBarrierException, IOException;
    protected abstract void _profile() throws InterruptedException, DatabaseException, BrokenBarrierException, IOException;
    //The above is implemented by the bolt(spout)Thread and called by the below
    void profile_routing(Platform p) throws InterruptedException, DatabaseException, BrokenBarrierException {}
    void routing() throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        if(start){
            cnt=0;
            start_emit=System.nanoTime();
        }
        while(running){
            _execute();
        }
        end_emit = System.nanoTime();
    }
    //end

    //public methods
    void Ready(Logger LOG){ ready=true; }
    public boolean isReady(){ return ready;}
    public int getExecutorID(){return executor.getExecutorID();}
    public String getOP(){return executor.getOP();}
    public double getCnt(){
        return cnt;
    }
    //end

}
