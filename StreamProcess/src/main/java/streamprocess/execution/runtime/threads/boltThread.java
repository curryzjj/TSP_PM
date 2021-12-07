package streamprocess.execution.runtime.threads;

import System.util.Configuration;
import ch.usi.overseer.OverHpc;
import engine.Clock;
import engine.Exception.DatabaseException;
import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinitySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.executor.BoltExecutor;
import streamprocess.components.topology.TopologyContext;
import streamprocess.controller.input.InputStreamController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.optimization.OptimizationManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

import static UserApplications.CONTROL.enable_numa_placement;
import static UserApplications.CONTROL.enable_shared_state;
import static net.openhft.affinity.AffinityLock.dumpLocks;

public class boltThread extends executorThread{
    private final static Logger LOG = LoggerFactory.getLogger(boltThread.class);
    private final BoltExecutor bolt;
    private final OutputCollector collector;
    private final InputStreamController scheduler;
    public volatile boolean binding_finished = false;
    private boolean UNIX = false;
    private int miss = 0;

    public boltThread(ExecutionNode e,  TopologyContext context,Configuration conf, long[] cpu,
                      int node, CountDownLatch latch, OverHpc HPCMonotor, OptimizationManager optimizationManager,
                      HashMap<Integer, executorThread> threadMap, Clock clock) {
        super(e, conf, context, cpu, node, latch, HPCMonotor, threadMap);
        bolt = (BoltExecutor) e.op;
        scheduler = e.getInputStreamController();
        this.collector = new OutputCollector(e, context);
        batch = conf.getInt("batch", 100);
        bolt.setExecutionNode(e);
        bolt.setclock(clock);
    }

    /**
     * Get input from upstream bolts, this is a unique function of bolt thread.
     * TODO: need a txn module to determine the fetch sequence.
     *
     * @since 0.0.7 we add a tuple txn module so that we can support customized txn rules in Brisk.execution.runtime.tuple fetching.
     */
    private Object fetchResult() {
        return scheduler.fetchResults();
    }
    private <STAT> JumboTuple fetchResult(STAT stat, int batch) {
        //implemented in the InputStreamController
        return scheduler.fetchResults(batch);
    }
    @Override
    protected void _execute_noControl() throws InterruptedException, DatabaseException, BrokenBarrierException {
        Object tuple=fetchResult();
        if(tuple==null){
            miss++;
        }else{
            if(tuple instanceof Tuple){
                if(tuple!=null){
                    bolt.execute((Tuple) tuple);
                    cnt+=1;
                }else{
                    miss++;
                }
            }else{
                if(tuple!=null){
                    bolt.execute((JumboTuple) tuple);
                    cnt+=batch;
                }else{
                    miss=miss+batch;
                }
            }
        }
    }

    @Override
    protected void _execute() throws InterruptedException, DatabaseException, BrokenBarrierException {
        _execute_noControl();
    }

    @Override
    protected void _profile() throws InterruptedException, DatabaseException, BrokenBarrierException {

    }

    @Override
    public void run() {
        try{
            Thread.currentThread().setName("Operator:"+executor.getOP()+"\tExecutor ID:"+executor.getExecutorID());
            Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
            initilize_queue(this.executor.getExecutorID());
            bolt.prepare(conf,context,collector);
            long[] binding=null;
            if (!conf.getBoolean("NAV",true)){
                binding=binding();
            }
            if(enable_numa_placement&&enable_shared_state&&!this.executor.isLeafNode()){
                if(conf.getBoolean("Sequential_Binding",true)){
                    binding=sequential_binding();
                }
            }
            this.Ready(LOG);
            if (enable_shared_state){
                if (!this.executor.isLeafNode())//TODO: remove such hard code in future.
                    bolt.loadDB(conf, context, collector);
                LOG.info("Operator:\t"+executor.getOP_full()+"is ready"+"\nlock_ratio dumps\n"+dumpLocks());
            }else{
                if(conf.getBoolean("NAV",true)){
                    LOG.info("Operator:\t" + executor.getOP_full() + " is ready");
                }
            }
            if (binding != null) {
                LOG.info("Successfully create boltExecutors " + bolt.getContext().getThisTaskId()
                        + "\tfor bolts:" + executor.getOP()
                        + " on node: " + node
                        + "binding:" + Long.toBinaryString(0x1000000000000000L | binding[0]).substring(1)
                );
            }
            binding_finished=true;
            latch.countDown();//tells others I'm ready
            try {
                latch.await();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
            if(this.executor.needsProfile()){
                profile_routing(context.getGraph().topology.getPlatform());
            }else{
                LOG.info("Operator:\t" + executor.getOP_full() + " starts");
                routing();
            }
        } catch (BrokenBarrierException |InterruptedException|DatabaseException e) {
            e.printStackTrace();
        } finally {
            if (lock != null) {
                lock.release();
            }
            this.executor.display();
            if (end_emit == 0) {
                end_emit = System.nanoTime();
            }
            double actual_throughput = cnt * 1E6 / (end_emit - start_emit);
            if(TopologyContext.plan.getSP()!=null){
                for (String Istream : new HashSet<>(executor.operator.input_streams)) {
                    //some function to calculate the expected_throughput
                }
            }
            if (expected_throughput == 0) {
                expected_throughput = actual_throughput;
            }
            LOG.info(this.executor.getOP_full()
                            + "\tfinished execution and exist with  throughput (k input_event/s) of:\t"
                            + actual_throughput + "(" + (actual_throughput / expected_throughput) + ")"
                            + " on node: " + node + " fetch miss rate:" + miss / (cnt + miss) * 100+" cnt="+cnt
            );
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {

            }
        }
    }
}
