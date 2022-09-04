package streamprocess.components.operators.executor;

import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.clr.CausalService;
import streamprocess.faulttolerance.clr.RecoveryDependency;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface IExecutor extends Serializable {
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    int getID();
    String getConfigPrefix();
    TopologyContext getContext();
    void display();
    /**
     * Called when an executor is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     */
    void callback(int callee, Tuple message);
    void setExecutionNode(ExecutionNode e);
    void clean_status();
    //Recovery or Undo Commit
    void ackCommit(boolean isRecovery, long alignMarkerId);
    //Checkpoint Commit
    void ackCommit(long offset);
    RecoveryDependency ackRecoveryDependency();
    ConcurrentHashMap<Integer, CausalService> ackCausalService();
    void recoveryInput(long offset, List<Integer> recoveryExecutorIDs, long alignOffset) throws FileNotFoundException, InterruptedException;
    int getStage();
    boolean IsStateful();
    double getEmpty();
}

