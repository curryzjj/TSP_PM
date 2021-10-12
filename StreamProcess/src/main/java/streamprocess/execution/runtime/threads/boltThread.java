package streamprocess.execution.runtime.threads;

import System.util.Configuration;
import ch.usi.overseer.OverHpc;
import streamprocess.components.operators.executor.BoltExecutor;
import streamprocess.components.topology.TopologyContext;
import streamprocess.controller.input.InputStreamController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

public class boltThread extends executorThread{

    private final BoltExecutor bolt;
    private final OutputCollector collector;
    private final InputStreamController scheduler;
    public volatile boolean binding_finished = false;
    private boolean UNIX = false;
    private int miss = 0;

    public boltThread(ExecutionNode e, Configuration conf, TopologyContext context, long[] cpu,
                      int node, CountDownLatch latch, OverHpc HPCMonotor, /*OptimizationManager optimizationManager,*/
                      HashMap<Integer, executorThread> threadMap, ExecutionNode executor/*Clock clock,*/) {
        super(e, conf, context, cpu, node, latch, HPCMonotor, threadMap);
        bolt = (BoltExecutor) e.op;
        this.collector=new OutputCollector(e,context);
        this.scheduler=e.getInputStreamController();
        //set bolt
    }

    /**
     * Get input from upstream bolts, this is a unique function of bolt thread.
     * TODO: need a txn module to determine the fetch sequence.
     *
     * @since 0.0.7 we add a tuple txn module so that we can support customized txn rules in Brisk.execution.runtime.tuple fetching.
     */
    private Object fetchResult() {
        //implemented in the InputStreamController
        return scheduler.fetchResults();
    }
    private <STAT> JumboTuple fetchResult(STAT stat, int batch) {
        //implemented in the InputStreamController
        return scheduler.fetchResults(batch);
    }
    @Override
    protected void _execute_noControl() throws InterruptedException, Exception, BrokenBarrierException {
        Object tuple=fetchResult();
        bolt.execute((Tuple) tuple);
        bolt.execute((JumboTuple) tuple);
    }

    @Override
    protected void _execute() throws InterruptedException, Exception, BrokenBarrierException {
        _execute_noControl();
    }

    @Override
    protected void _profile() throws InterruptedException, Exception, BrokenBarrierException {

    }

    @Override
    public void run() {
    }
}
