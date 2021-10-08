package streamprocess.components.topology;

import ch.usi.overseer.OverHpc;
import engine.Database;
import streamprocess.components.grouping.Grouping;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.threads.executorThread;
import streamprocess.optimization.ExecutionPlan;

import java.util.*;

/**
 * A TopologyContext is created for each executor.
 * It is given to bolts and spouts in their "Loading" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the StreamProcess.topology, such as Task ids, inputs and outputs, etc.
 * <profiling/>
 */
public class TopologyContext {
    public static ExecutionPlan plan;
    public static OverHpc HPCMonotor;
    private static ExecutionGraph graph;
    private static Database db;
    private static HashMap<Integer, executorThread> threadMap;
    private final int _taskId;
    /**
     * Instead of Store Brisk.topology, we Store Brisk.execution graph directly!
     * This is a global access memory structure,
     */
    public TopologyContext(ExecutionGraph g, Database db, ExecutionPlan plan, ExecutionNode executor, HashMap<Integer,executorThread> threadMap, OverHpc HPCMonotor){
        TopologyContext.plan=plan;
        TopologyContext.graph=g;
        TopologyContext.db=db;
        TopologyContext.threadMap=threadMap;
        TopologyContext.HPCMonotor=HPCMonotor;
        this._taskId=executor.getExecutorID();
    }

    public HashMap<String,Map<TopologyComponent, Grouping>> getThisSources(){
       return this.getComponent(this.getThisComponentId()).getParents();
    }
    public String getThisComponentId(){return this.getComponentId(this._taskId);}
    private String getComponentId(int taskId) {
        return getComponent(taskId).getId();
    }
    private TopologyComponent getComponent(String component) {
        return graph.topology.getComponent(component);
    }



    public TopologyComponent getThisComponent() {
        return this.getComponent(this._taskId);
    }
    public TopologyComponent getComponent(int taskId) { return graph.getExecutionNodeArrayList().get(taskId).operator;}
    public Database getDb() {
        return db;
    }

    public int getThisTaskIndex() {
        return 0;
    }
}
