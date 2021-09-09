package streamprocess.execution.runtime.threads;

import System.util.Configuration;
import ch.usi.overseer.OverHpc;
import streamprocess.components.operators.executor.BasicSpoutBatchExecutor;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

public class spoutThread extends executorThread{
    private final BasicSpoutBatchExecutor sp;

    protected spoutThread(ExecutionNode e, Configuration conf, TopologyContext context, long[] cpu,
                          int node, CountDownLatch latch, OverHpc HPCMonotor,
                          HashMap<Integer, executorThread> threadMap, ExecutionNode executor/*,Clock clock*/) {
        super(e, conf, context, cpu, node, latch, HPCMonotor, threadMap);
        this.sp=(BasicSpoutBatchExecutor) e.op;
        //some config
    }

    @Override
    protected void _execute_noControl() throws InterruptedException, Exception, BrokenBarrierException {
        sp.bulk_emit(batch);
    }

    @Override
    protected void _execute() throws InterruptedException, Exception, BrokenBarrierException {
        _execute_noControl();
    }

    @Override
    protected void _profile() throws InterruptedException, Exception, BrokenBarrierException {
        //Some conditions
        sp.bulk_emit(batch);
    }
    @Override
    public void run() {

    }
}
