package streamprocess.optimization;

import System.Platform.Platform;
import System.util.Configuration;
import engine.Database;
import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.components.topology.Topology;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionManager;
import streamprocess.execution.ExecutionPlan;
import streamprocess.faulttolerance.checkpoint.CheckpointManager;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static UserApplications.CONTROL.enable_checkpoint;

public class OptimizationManager extends Thread {
    private final static Logger LOG = LoggerFactory.getLogger(OptimizationManager.class);
    private final Configuration conf;
    private final int end_cnt = 50;//50* 10=500 seconds per executor maximally
    private final long warmup_gaps = (long) (60 * 1E3);//60 seconds.
    private final boolean profile;
    //private final RoutingOptimizer ro;
    private final String prefix;
    public int start_executor = 0;
    public int end_executor = 1;
    public ExecutionGraph g;
    //private Optimizer so;
    private ExecutionPlan executionPlan;
    private ExecutionManager EM;
    private CheckpointManager CM;
    public CountDownLatch latch;
    private long profiling_gaps = 10000;//10 seconds.
    private int profile_start = 0;
    private int profile_end = 1;
    public int node;
    private Topology topology;
    public OptimizationManager(ExecutionGraph g, Configuration conf, boolean profile, double relex, Platform P){
        this.g=g;
        this.conf=conf;
        this.profile=profile;
        prefix = conf.getConfigPrefix();
        //TODO:add RoutingOptimizer and Optimizer
    }
    public ExecutionManager getEM(){
        return EM;
    }
    public ExecutionPlan launch(Topology topology, Platform p, Database db) throws UnhandledCaseException, IOException {
        this.topology=topology;
        final String initial_locks= AffinityLock.dumpLocks();
        boolean nav = conf.getBoolean("NAV", true);
        boolean benchmark = conf.getBoolean("benchmark", false);
        boolean manual = conf.getBoolean("manual", false);
        boolean load = conf.getBoolean("Prepared", false);
        boolean parallelism_tune = conf.getBoolean("parallelism_tune", false);
        EM=new ExecutionManager(g,conf,this,db,p);
        latch = new CountDownLatch(g.getExecutionNodeArrayList().size() + 1 - 1);//+1:OM -1:virtual
        if(enable_checkpoint){
           CM=new CheckpointManager(g,conf,db);
        }
        if(nav){
            LOG.info("Native execution");
            executionPlan=new ExecutionPlan(null,null);
            executionPlan.setProfile();
            EM.distributeTasks(conf,executionPlan,latch,false,false,db,p,CM);
        }
        final String dumpLocks = AffinityLock.dumpLocks();
        return executionPlan;
    }

    @Override
    public void run() {
        this.node=0;
        latch.countDown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            /**
             * TODO:profile code
             * TODO:optimize code
             */
            LOG.info("Optimization manager exists");
        }
    }
}
