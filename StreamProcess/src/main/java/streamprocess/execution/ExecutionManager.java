package streamprocess.execution;

import System.Platform.Platform;
import System.util.Configuration;
import UserApplications.CONTROL;
import ch.usi.overseer.OverHpc;
import engine.Clock;
import engine.Database;
import engine.transaction.TxnProcessingEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.threads.boltThread;
import streamprocess.execution.runtime.threads.executorThread;
import streamprocess.execution.runtime.threads.spoutThread;
import streamprocess.faulttolerance.FTManager;
import streamprocess.faulttolerance.checkpoint.CheckpointManager;
import streamprocess.faulttolerance.recovery.RecoveryManager;
import streamprocess.optimization.OptimizationManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static System.constants.BaseConstants.BaseStream.*;
import static UserApplications.CONTROL.*;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;
import static engine.Database.snapshotExecutor;
import static engine.log.WALManager.writeExecutor;

public class ExecutionManager {
    private final static Logger LOG = LoggerFactory.getLogger(ExecutionManager.class);
    private final static long migration_gaps = 10000;
    public static Clock clock = null;//used in the shapshot
    TxnProcessingEngine tp_engine;
    FTManager FTM;
    RecoveryManager RM;
    private static Thread checkpointManagerThread;
    private static Thread recoveryManagerThread;
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
    public void  distributeTasks(Configuration conf, ExecutionPlan plan, CountDownLatch latch, boolean benchmark,
                                boolean profile, Database db, Platform p, FTManager FTM,RecoveryManager RM) throws UnhandledCaseException, IOException {
        assert plan !=null;
        loadTargetHz =(int) conf.getDouble("targetHz",10000000);
        LOG.info("Finally, targetHZ set to:" + loadTargetHz);
        timeSliceLengthMs = conf.getInt("timeSliceLengthMs");
        g.build_inputSchedule();
        clock = new Clock(conf.getDouble("shapshot", 1));
        if(enable_snapshot||enable_wal){
            this.startFaultTolerance(RM,FTM);
        }
        if (enable_shared_state){
            HashMap<Integer, List<Integer>> stage_map = new HashMap<>();
            for (ExecutionNode e : g.getExecutionNodeArrayList()) {
                stage_map.putIfAbsent(e.op.getStage(), new LinkedList<>());
                stage_map.get(e.op.getStage()).add(e.getExecutorID());
            }
            int stage = 0;//currently only stage 0 is required..
            List<Integer> integers = stage_map.get(stage);
            tp_engine=TxnProcessingEngine.getInstance();
            tp_engine.initialize(integers.size(), conf.getString("application"));
            tp_engine.engine_init(
                    integers.get(0),
                    integers.get(integers.size() - 1),
                    integers.size(),
                    conf.getInt("TP",10));
            if(enable_wal&&enable_parallel){
                writeExecutor= Executors.newFixedThreadPool(integers.size());
            }else if(enable_parallel&&enable_snapshot){
                snapshotExecutor=Executors.newFixedThreadPool(integers.size());
            }
            int delta = (int) Math.ceil(NUM_SEGMENTS / (double)integers.size());
            db.setCheckpointOptions(integers.size(),delta);
        }
        executorThread thread = null;
        for (ExecutionNode e : g.getExecutionNodeArrayList()) {
            switch(e.operator.type){
                case spoutType:thread=launchSpout_SingleCore(e,new TopologyContext(g,db,plan,e,ThreadMap,HPCMonotor,FTM,RM),conf,plan.toSocket(e.getExecutorID()),latch);
                break;
                case boltType:
                case sinkType:thread=launchBolt_SingleCore(e,new TopologyContext(g,db,plan,e,ThreadMap,HPCMonotor,FTM,RM),conf,plan.toSocket(e.getExecutorID()),latch);
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
        while(checkpointManagerThread.isAlive()){
            checkpointManagerThread.interrupt();
        }
        if(CONTROL.enable_shared_state&&tp_engine!=null){
            tp_engine.engine_shutdown();
            if(enable_parallel&&enable_wal){
                writeExecutor.shutdown();
            }else if(enable_parallel&&enable_snapshot){
                snapshotExecutor.shutdown();
            }
        }
        this.getSinkThread().getContext().Sequential_stopAll();
    }
    public executorThread getSinkThread(){return ThreadMap.get(g.getSinkThread());}
    public executorThread getSpoutThread(){
        return ThreadMap.get(g.getSpoutThread());
    }
    public void startFaultTolerance(RecoveryManager RM,FTManager FTM) throws IOException {
        this.RM=RM;
        RM.initialize(false);
        recoveryManagerThread=new Thread(RM);
        RM.start();
        this.FTM=FTM;
        FTM.initialize(RM.needRecovery());
        checkpointManagerThread=new Thread(FTM);
        FTM.start();
    }
    public void closeFTM() {
        FTM.running=false;
        Object lock=FTM.getLock();
        FTM.close();
        synchronized (lock){
            lock.notifyAll();
        }
    }
}
