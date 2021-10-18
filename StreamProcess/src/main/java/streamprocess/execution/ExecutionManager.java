package streamprocess.execution;

import System.Platform.Platform;
import System.util.Configuration;
import UserApplications.CONTROL;
import ch.usi.overseer.OverHpc;
import engine.Clock;
import engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.threads.boltThread;
import streamprocess.execution.runtime.threads.executorThread;
import streamprocess.execution.runtime.threads.spoutThread;
import streamprocess.optimization.OptimizationManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import static System.Constants.*;

public class ExecutionManager {
    private final static Logger LOG = LoggerFactory.getLogger(ExecutionManager.class);
    private final static long migration_gaps = 10000;
    //wait for transaction process
    public static Clock clock = null;//used in the checkpoint
    //TxnProcessingEngine tp_engine;
    public final HashMap<Integer, executorThread> ThreadMap = new HashMap<>();
    //public final AffinityController AC;//not sure
    private final OptimizationManager optimizationManager;
    private int loadTargetHz;
    private int timeSliceLengthMs;
    private OverHpc HPCMonotor;
    private ExecutionGraph g;
    private boolean Txn_lock = true;

    public ExecutionManager(ExecutionGraph g, Configuration conf, OptimizationManager optimizationManager, Database db, Platform p){
        this.g=g;
        //AC = new AffinityController(conf, p);
        this.optimizationManager = optimizationManager;
        initializeHPC();
    }
    private void initializeHPC() {}//not sure
    /**
     * Launch threads for each executor in executionGraph
     * All executors have to sync_ratio for OM to start, so it's safe to do initialization here. E.g., initialize database.
     */
    public void distributeTasks(Configuration conf, ExecutionPlan plan, CountDownLatch latch, boolean benchmark,
                                boolean profile, Database db, Platform p) throws UnhandledCaseException{
        assert plan !=null;
        loadTargetHz =(int) conf.getDouble("targetHz",10000000);
        LOG.info("Finally, targetHZ set to:" + loadTargetHz);
        timeSliceLengthMs = conf.getInt("timeSliceLengthMs");
        g.build_inputSchedule();
        //TODO:support the FaultTolerance
        //TODO:support the TxnprocessEngine
        executorThread thread = null;
        for (ExecutionNode e : g.getExecutionNodeArrayList()) {
            switch(e.operator.type){
                case spoutType:thread=launchSpout_SingleCore(e,new TopologyContext(g,db,plan,e,ThreadMap,HPCMonotor),conf,plan.toSocket(e.getExecutorID()),latch);
                break;
                case boltType:
                case sinkType:thread=launchBolt_SingleCore(e,new TopologyContext(g,db,plan,e,ThreadMap,HPCMonotor),conf,plan.toSocket(e.getExecutorID()),latch);
                break;
                case virtualType:
                    LOG.info("Won't launch virtual ground");
                    break;
                default:
                    throw new UnhandledCaseException("type not recognized");
            }
        }
    }
    private executorThread launchSpout_InCore(ExecutionNode e, TopologyContext context,Configuration conf,int node,long[] cores,CountDownLatch latch){
       spoutThread st;
       st=new spoutThread(e,context,conf,cores,node,latch,loadTargetHz,timeSliceLengthMs,HPCMonotor,ThreadMap,clock);
       st.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            st.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(),st);
        return st;
    }
    private executorThread launchSpout_SingleCore(ExecutionNode e, TopologyContext context, Configuration conf,
                                                  int node, CountDownLatch latch){
        long[] cpu;
        if(!conf.getBoolean("NAV",true)){
            //implement after AC
            cpu=new long[2];
        }else{
            cpu=new long[1];
        }
        return launchSpout_InCore(e,context,conf,node,cpu,latch);
    }
    private executorThread launchBolt_InCore(ExecutionNode e,TopologyContext context,Configuration conf,int node,long[] cores,CountDownLatch latch){
        boltThread bt;
        bt=new boltThread(e,context,conf,cores,node,latch,HPCMonotor,optimizationManager,ThreadMap,clock);
        bt.setDaemon(true);
        if (!(conf.getBoolean("monte", false) || conf.getBoolean("simulation", false))) {
            bt.start();
        }
        ThreadMap.putIfAbsent(e.getExecutorID(), bt);
        return bt;
    }
    private executorThread launchBolt_SingleCore(ExecutionNode e,TopologyContext context,Configuration conf,int node,CountDownLatch latch){
        long cpu[];
        if (!conf.getBoolean("NAV", true)) {
            //implement after AC
            cpu=new long[2];
        } else {
            cpu = new long[1];
        }
        return launchBolt_InCore(e, context, conf, node, cpu, latch);
    }
    public void redistributeTasks(ExecutionGraph g,Configuration conf, ExecutionPlan plan) throws InterruptedException{}
    public void exit() throws IOException {
        LOG.info("Execution stops");
        if(clock!=null){
            clock.close();
        }
        this.getSinkThread().getContext().Sequential_stopAll();
        if(CONTROL.enable_shared_state/*&&tp_engine!=null*/){
            //stop the tp_engine
        }
    }
    public executorThread getSinkThread(){return ThreadMap.get(g.getSinkThread());}

    public void exist() {
        LOG.info("Execution stops.");
        LOG.info("ExecutionManager is going to stop all threads sequentially");
        this.getSinkThread().getContext().Sequential_stopAll();//Only one sink will do the measure_end
    }
}
