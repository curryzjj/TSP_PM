package streamprocess.components.topology;

import streamprocess.components.grouping.Grouping;
import streamprocess.components.operators.executor.IExecutor;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.streaminfo;

import java.io.Serializable;
import java.util.*;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;


/**
 * Helper class. SchemaRecord all necessary information about a spout/bolt.
 */
public abstract class TopologyComponent implements Serializable {
    public final char type;
    /**
     * One can register to multiple input streams.
     */
    public final ArrayList<String> input_streams;
    final Grouping[] groups;//not sure
    /**
     * One can emit multiple output_streams with different output data (and/or different fields).
     * < StreamID, Streaminfo >
     */
    final HashMap<String, streaminfo> output_streams;
    private final String id;//the name of the operator
    private final IExecutor op;//this is the operator structure passed in from user application
    private final HashMap<String, HashMap<String, Grouping>> grouping_to_downstream;//not sure
    private ArrayList<ExecutionNode> executor;//executor of this operator is initiated after execution graph is created.
    private ArrayList<Integer> executorID;//executor of this operator is initiated after execution graph is created.
    public boolean toCompress = true;
    private int numTasks;

    public TopologyComponent(HashMap<String, streaminfo> output_streams, String id, char type, IExecutor op, int numTasks,
                             ArrayList<String> input_streams, Grouping... groups) {
        //set by the TopologyBuilder
        this.type = type;
        this.input_streams = input_streams;
        this.groups = groups;
        this.output_streams = output_streams;
        this.id = id;
        this.op = op;
        this.numTasks = numTasks;
        //end
        executor = new ArrayList<>();
        executorID = new ArrayList<>();
        grouping_to_downstream = new LinkedHashMap<>();
    }

    void clean(){
        executor=new ArrayList<>();
        executorID=new ArrayList<>();
    }
    public HashMap<String, streaminfo> getOutput_streams() {
        return output_streams;
    }
    public Grouping getGrouping_to_downstream(String downOp, String streamId) {
        return grouping_to_downstream.get(downOp).get(streamId);
    }
    public ArrayList<ExecutionNode> getExecutorList() {
        return executor;
    }
    public ArrayList<Integer> getExecutorIDList() {
        return executorID;
    }
    public IExecutor getOp() {
        return op;
    }
    public String getId() { return id; }
    public int getNumTasks() {
        return numTasks;
    }
    public void setNumTasks(int numTasks) {
        this.numTasks = numTasks;
    }
    public int getFID() {
        return op.getStage();
    }
    public void link_to_executor(ExecutionNode vertex) {
        executor.add(vertex);
        executorID.add(vertex.getExecutorID());
    }
    public void setGrouping(String downOp, Grouping g) {
        String stream=g.getStreamID();
        this.grouping_to_downstream.computeIfAbsent(downOp,k->new HashMap<>());
        this.grouping_to_downstream.get(downOp).put(stream,g);
    }
    public Fields get_output_fields(String sourceStreamId) {
        return output_streams.get(sourceStreamId).getFields();
    }
    public Map<TopologyComponent, Grouping> getChildrenOfStream() {
        return getChildrenOfStream(DEFAULT_STREAM_ID);
    }
    //implement by the multi_component
    public abstract void setChildren(TopologyComponent topologyComponent, Grouping g);
    public abstract void setParents(TopologyComponent topologyComponent, Grouping g);
    public abstract Set<String> getOutput_streamsIds();

    public abstract Map<TopologyComponent, Grouping> getChildrenOfStream(String streamId);

    public abstract Map<TopologyComponent, Grouping> getParentsOfStream(String streamId);

    public abstract HashMap<String, Map<TopologyComponent, Grouping>> getParents();

    public abstract boolean isLeafNode();

    public abstract boolean isLeadNode();
    public abstract Set<String> get_childrenStream();
    //end
}
