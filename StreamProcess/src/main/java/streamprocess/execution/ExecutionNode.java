package streamprocess.execution;

import System.Platform.Platform;
import streamprocess.components.operators.executor.IExecutor;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.input.InputStreamController;
import streamprocess.controller.output.OutputController;

import java.io.Serializable;
import java.util.*;

/**
 * ExecutionNode support multiple input(receive_queue) and output streams(through stream partition)
 */

public class ExecutionNode implements Serializable {
    public final TopologyComponent operator;//Operator structure shouldn't be referenced as tp_engine information.....
    private final int executorID;//global ID for this executorNode in current topology
    public final IExecutor op;//Operator should be owned by executionNode as it maintains unique information such as context information.
    private InputStreamController inputStreamController;
    public final int compressRatio;
    private OutputController controller;
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> parents = new HashMap();
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> children = new HashMap();
    private boolean last_executor=false;
    private boolean first_executor;
    private boolean needsProfile;

    public ExecutionNode(TopologyComponent rec, int i, Platform p, int compressRatio) {
        this.operator = rec;
        this.executorID = i;
        op = null;
        this.compressRatio = compressRatio;
    }
    public ExecutionNode(TopologyComponent rec, int i, Platform p) {
        this.operator = rec;
        this.executorID = i;
        op = null;
        compressRatio = 1;
    }

    public ExecutionNode(ExecutionNode e, TopologyComponent topo, Platform platform) {
        this.operator = topo;
        this.executorID = e.getExecutorID();
        op = null;
        compressRatio = e.compressRatio;
    }

    //First executor: what is the different->use the first executor as the profiling target
    public boolean isFirst_executor() { return first_executor; }
    public void setFirst_executor(boolean first_executor) {
        this.first_executor = first_executor;
    }
    //end

    //custom inputStreamController for this execution mapping_node.
    //How about the inputStreamController in the Topology
    public boolean hasScheduler() {
        return inputStreamController != null;
    }
    public InputStreamController getInputStreamController() {
        return inputStreamController;
    }
    public void setInputStreamController(InputStreamController inputStreamController) {
        this.inputStreamController = inputStreamController;
    }
    //end

    //the OutputController
    public void setController(OutputController controller) {
        this.controller = controller;
    }
    public OutputController getController() {
        return controller;
    }
    //end

    //get operatorID from TopologyComponent
    public String getOP() { return operator.getId(); }
    public String getOP_full() { return operator.getId() + "(" + executorID + ")"; }
    //end

    //Parents, Children, Source and Leaf nodes
    public HashMap<TopologyComponent, ArrayList<ExecutionNode>> getParents() {
        return parents;
    }
    public HashMap<TopologyComponent, ArrayList<ExecutionNode>> getChildren() {
        return children;
    }
    public Set<TopologyComponent> getParents_keySet() {
        return parents.keySet();
    }
    public Set<TopologyComponent> getChildren_keySet() {
        return children.keySet();
    }
    public ArrayList<ExecutionNode> getParentsOf(TopologyComponent operator) {
        ArrayList<ExecutionNode> executionNodes = parents.get(operator);
        if (executionNodes == null) {
            executionNodes = new ArrayList<>();
        }
        return executionNodes;
    }
    public ArrayList<ExecutionNode> getChildrenOf(TopologyComponent operator) {
        return children.get(operator);
    }
    public boolean isSourceNode() {
        return true;
    }
    public boolean isLeafNode() {
        return true;
    }
    private boolean isLeadNode() { return true; }
    //end



    public int getExecutorID() {
        return 0;
    }

    public void setLast_executorOfBolt(boolean last_executor) {
        this.last_executor=last_executor;
    }

    public void setNeedsProfile() {
        this.needsProfile = true;
    }
}
