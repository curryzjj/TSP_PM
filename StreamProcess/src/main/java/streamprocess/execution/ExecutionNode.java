package streamprocess.execution;

import System.Platform.Platform;
import com.oracle.tools.packager.Log;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.executor.IExecutor;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.input.InputStreamController;
import streamprocess.controller.output.OutputController;
import streamprocess.controller.output.PartitionController;
import streamprocess.execution.runtime.collector.OutputCollector;

import java.io.Serializable;
import java.util.*;

/**
 * ExecutionNode support multiple input(receive_queue) and output streams(through stream partition)
 */

public class ExecutionNode implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionNode.class);
    public final TopologyComponent operator;//Operator structure shouldn't be referenced as tp_engine information.....
    //Below are self maintain structures.
    public final IExecutor op;//Operator should be owned by executionNode as it maintains unique information such as context information.
    //public final RateModel RM;
    //model related structures. Source executor Id, related model.
    //public final HashMap<Integer, STAT> profiling = new HashMap<>();
    public final int compressRatio;
    private final int executorID;//global ID for this executorNode in current Brisk.topology
    //    public HashMap<Integer, Boolean> BP=new HashMap<>();//Backpressure corrected.
    private final boolean BP = false;
    //Below are created later.
    private OutputController controller;//TODO: it should be initialized during Brisk.topology compile, which is after current Brisk.execution SimExecutionNode being initialized, so it can't be made final.
    private InputStreamController inputStreamController;//inputStreamController is initialized even after partition.
    private boolean last_executor = false;//if this executor is the last executor of operator
    private boolean activate = true;
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> parents = new HashMap();
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> children = new HashMap();
    //private boolean _allocated = false;
    private boolean _statistics;
    private boolean first_executor;
    private boolean needsProfile;

    public ExecutionNode(TopologyComponent rec, int i, Platform p, int compressRatio) {
        this.operator = rec;
        this.executorID = i;
        this.compressRatio = compressRatio;
        IExecutor op=rec.getOp();
        if(op!=null){
            this.op= (IExecutor) SerializationUtils.clone(op);
        }else{
            this.op=null;
        }
        parents=new HashMap<>();
        children=new HashMap<>();
        needsProfile=false;
    }
    public ExecutionNode(TopologyComponent rec, int i, Platform p) {
        this.operator = rec;
        this.executorID = i;
        compressRatio = 1;
        // create an tp_engine of Operator through serialization.
        // We need to do this because each Brisk.execution thread should own its own context, which is hold by the Operator structure!
        IExecutor op = rec.getOp();
        if (op != null) {
            this.op = org.apache.commons.lang3.SerializationUtils.clone(op);
        } else {
            this.op = null;
        }
        needsProfile = false;
    }

    public ExecutionNode(ExecutionNode e, TopologyComponent topo, Platform platform) {
        compressRatio = e.compressRatio;
        this.operator = topo;
        this.executorID = e.executorID;
        this.first_executor = e.first_executor;
        this.last_executor = e.last_executor;
        // create an tp_engine of Operator through serialization.
        // We need to do this because each Brisk.execution thread should own its own context, which is hold by the Operator structure!
        IExecutor op = operator.getOp();
        if (op != null) {
            this.op = org.apache.commons.lang3.SerializationUtils.clone(op);
        } else {
            this.op = null;
        }
        parents = new HashMap();
        children = new HashMap();
        needsProfile = e.needsProfile;
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
    public boolean isEmpty() {
        return getController() == null || getController().isEmpty();
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
    public boolean noParents() {
        final Set<TopologyComponent> parents_set = this.getParents_keySet();//multiple parent
        return parents_set.size() == 0;
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
    //Output_queue
    public void setReceive_queueOfChildren(String streamId){//used int the executorThread
        if(!isLeafNode()){
            final OutputController controller=getController();
            if(!controller.isShared()||this.isLeafNode()){
                //for each downstream Operator ID
                if(operator.getChildrenOfStream(streamId)!=null){
                    for(TopologyComponent op:this.operator.getChildrenOfStream(streamId).keySet()){
                        for(ExecutionNode downstream_executor:op.getExecutorList()){
                            int executorID=this.getExecutorID();
                            //GetAndUpdate output queue from output partition
                            final Queue queue=this.getController().getPartitionController(streamId,op.getId()).get_queue(downstream_executor.getExecutorID());
                            downstream_executor.inputStreamController.setReceive_queue(streamId,executorID,queue);
                        }
                    }
                }
            }
        }else{
            LOG.info("Executor:"+this.getOP_full()+"have no children for stream"+streamId);
        }
        if(this.inputStreamController!=null){
            this.inputStreamController.initialize();
        }
    }
    public void allocate_OutputQueue(boolean linked,int desired_elements_epoch_per_core){
        if(!isLeafNode()){
            final OutputController controller=getController();
            if(controller.isShared()){//MPSC
                if(this.isLeadNode()){
                    controller.allocatequeue(linked, desired_elements_epoch_per_core);
                }
            }else{//SPSC
                controller.allocatequeue(linked, desired_elements_epoch_per_core);
            }
        }
    }
    //GetAndUpdate partition ratio for the downstream executor
    public double getPartition_ratio(String input_stream_downOpID, String downOpID, int ExecutorID) {
        if (ExecutorID == -2) {
            return 1;//virtual ground.
        }
        Double partition_ratio = null;
        try {
            partition_ratio = controller.getPartitionController(input_stream_downOpID, downOpID).getPartition_ratio(ExecutorID);
        } catch (Exception e) {
            Collection<PartitionController> partitionControllers = controller.getPartitionController();
            for (PartitionController partitionController : partitionControllers) {
                LOG.info(partitionController.toString());
            }
            System.exit(-1);
        }
        return partition_ratio;
    }

    public int getExecutorID() {
        return 0;
    }
    public void setLast_executorOfBolt(boolean last_executor) {
        this.last_executor=last_executor;
    }
    public void setNeedsProfile() {
        this.needsProfile = true;
    }
    public boolean needsProfile() {
        return this.needsProfile;
    }
    public String toString() {
        return this.getOP();
    }
    public boolean isVirtual() {
        return executorID == -2;
    }

    public void display() {
        op.display();
    }
}
