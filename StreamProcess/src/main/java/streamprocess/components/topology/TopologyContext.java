package streamprocess.components.topology;

import ch.usi.overseer.OverHpc;
import engine.Database;
import streamprocess.components.grouping.Grouping;
import streamprocess.controller.input.InputStreamController;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.threads.executorThread;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.ExecutionPlan;

import java.util.*;

/**
 * A TopologyContext is created for each executor.
 * It is given to bolts and spouts in their "Loading" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the StreamProcess.topology, such as Task ids, inputs and outputs, etc.
 * <profiling/>
 * initialize in the executionManager at distributeTasks
 */
public class TopologyContext {
    public static ExecutionPlan plan;
    public static OverHpc HPCMonotor;
    private static ExecutionGraph graph;
    private static Database db;
    private static HashMap<Integer, executorThread> threadMap;
    private final int _taskId;//executorID
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


    //get context information
    public Database getDb() {
        return db;
    }
    public ExecutionGraph getGraph() {
        return graph;
    }
    public static ExecutionPlan getPlan() {
        return plan;
    }
    public int getThisTaskId() {
        return _taskId;
    }
    public int getThisTaskIndex() {
        return getThisTaskId();
    }
    //get group information
    public ExecutionNode getExecutor(int taskId){
        return graph.getExecutionNode(taskId);
    }
    public TopologyComponent getComponent(int taskId) {
        return graph.getExecutionNodeArrayList().get(taskId).operator;
    }
    private TopologyComponent getComponent(String componentId) {
        return graph.topology.getComponent(componentId);
    }
    public InputStreamController getScheduler() {
        return graph.getGlobal_tuple_scheduler();
    }
    public Fields getComponentOutputFields(String componentId, String sourceStreamId){
        TopologyComponent op=graph.topology.getComponent(componentId);
        return op.get_output_fields(sourceStreamId);
    }
    //get component information
    public HashMap<String,Map<TopologyComponent, Grouping>> getThisSources(){
        return this.getComponent(this._taskId).getParents();
    }
    public TopologyComponent getThisComponent() {
        return this.getComponent(this._taskId);
    }
    public String getThisComponentId(){
        return getComponent(this._taskId).getId();
    }
    public int getNUMTasks() {
        return this.getComponent(_taskId).getNumTasks();
    }
    //function
    public void wait_for_all() throws InterruptedException{
        for(int id:threadMap.keySet()){
            if(id!=getThisTaskId()){
                threadMap.get(id).join(10000);
            }
        }
    }
    public void stop_running(){
        threadMap.get(getThisTaskId()).running=false;
        threadMap.get(getThisTaskId()).interrupt();
    }
    public void stop_running(int TaskId){
        threadMap.get(TaskId).running=false;
        threadMap.get(TaskId).interrupt();
    }
    public HashMap<Integer, executorThread> getThreadMap(){
        return this.threadMap;
    }
    public void stop_runningALL(){
        for (int id : threadMap.keySet()) {
            if (id != getThisTaskId()) {
                threadMap.get(id).running = false;
                threadMap.get(id).interrupt();
            }
        }
    }
    public void force_existALL(){
        for(int id: threadMap.keySet()){
            if(id!=getThisTaskId()){
                while(threadMap.get(id).isAlive()){
                    threadMap.get(id).running=false;
                    threadMap.get(id).interrupt();
                }
            }
        }
    }
    public void Sequential_stopAll(){
        this.stop_runningALL();
        force_existALL();
        //stop myself.
        threadMap.get(getThisTaskId()).running = false;
        threadMap.get(getThisTaskId()).interrupt();
    }
}
