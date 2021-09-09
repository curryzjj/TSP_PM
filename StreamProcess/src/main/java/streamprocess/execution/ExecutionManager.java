package streamprocess.execution;

import System.Platform.Platform;
import System.util.Configuration;
import engine.Database;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.execution.runtime.threads.executorThread;
import streamprocess.execution.runtime.threads.spoutThread;
import streamprocess.optimization.ExecutionPlan;
import streamprocess.optimization.OptimizationManager;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import static System.Constants.*;

public class ExecutionManager {

    private final ExecutionGraph g;
    public final HashMap<Integer, executorThread> ThreadMap = new HashMap<>();
    private final OptimizationManager optimizationManager;
    //public final AffinityController AC;//not sure

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
        g.build_inputSchedule();
        //TODO:support the TxnprocessEngine
        executorThread thread = null;
        for (ExecutionNode e : g.getExecutionNodeArrayList()) {
            switch(e.operator.type){
                case spoutType: thread=launchSpout_InCore();
                break;
                case boltType:
                case sinkType:thread=launchBolt_InCore();
                break;
                case virtualType:
                    break;
                default:
                    throw new UnhandledCaseException("type not recognized");
            }
        }
    }
    private executorThread launchSpout_InCore(){
        spoutThread st=null;
        ExecutionNode e = null;
        st.setDaemon(true);
        st.start();
        ThreadMap.putIfAbsent(e.getExecutorID(),st);
        return st;
    }
    private executorThread launchBolt_InCore(){return null;}
    private executorThread launchSpout_SingleCore(){return null;}
    private executorThread launchBolt_SingleCore(){return null;}
    public void redistributeTasks(ExecutionGraph g,Configuration conf, ExecutionPlan plan) throws InterruptedException{}
    public void exit(){}
    public executorThread getSinkThread(){return ThreadMap.get(g.getSinkThread());}
}
