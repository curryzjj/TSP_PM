package streamprocess.components.operators.executor;

import System.util.Configuration;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Marker;

import java.io.Serializable;
import java.util.Map;

public interface IExecutor extends Serializable {
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    int getID();
    double get_read_selectivity();
    Map<String, Double> get_input_selectivity();
    Map<String, Double> get_output_selectivity();
    double get_branch_selectivity();
    String getConfigPrefix();
    TopologyContext getContext();
    void display();
    double getResults();
    double getLoops();
    boolean isScalable();
    /**
     * Called when an executor is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     */
    void cleanup();
    void callback(int callee, Marker marker);
    void setExecutionNode(ExecutionNode e);
    Integer default_scale(Configuration conf);
    //void configureWriter(Writer writer);
    //void configureLocker(OrderLock lock, OrderValidate orderValidate);
    void clean_state(Marker marker);
    int getStage();
    void earlier_clean_state(Marker marker);
    boolean IsStateful();
    void forceStop();
    double getEmpty();
}

