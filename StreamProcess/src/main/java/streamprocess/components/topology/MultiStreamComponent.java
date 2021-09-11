package streamprocess.components.topology;

import streamprocess.components.grouping.Grouping;
import streamprocess.components.operators.executor.IExecutor;
import streamprocess.execution.runtime.tuple.streaminfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MultiStreamComponent extends TopologyComponent {
    /**
     * New added:
     * 1) children: records the downstream operator of the current operator of the streamId.
     * 2) executor: all the executors this operator own.
     * < StreamId, DownstreamOP >
     */
    private final HashMap<String, Map<TopologyComponent, Grouping>> children;
    /**
     * New added:
     * 1) parents: records the upstream operator of the current operator of the streamId.
     * 2) executor: all the executors this operator own.
     * < StreamId, upstreamOP >
     */
    private final HashMap<String, Map<TopologyComponent, Grouping>> parents;
    public MultiStreamComponent(String id, char type, IExecutor op, int numTasks, ArrayList<String> input_streams,
                                HashMap<String, streaminfo> output_streams, Grouping... groups) {
        super(output_streams, id, type, op, numTasks, input_streams, groups);
        children = new HashMap<>();
        parents = new HashMap<>();
    }

    public MultiStreamComponent(TopologyComponent topo, Topology topology) {
        super(topo.output_streams, topo.getId(), topo.type, topo.getOp(), topo.getNumTasks(), topo.input_streams, topo.groups);
        children = new HashMap<>();
        parents = new HashMap<>();
        //for grouping implement after
        this.toCompress = topo.toCompress;
    }

    /*children and Grouping to downstream structures are "upload" by children operator.*/
    @Override
    public void setChildren(TopologyComponent downOp, Grouping g) {
        String stream=g.getStreamID();
        Map<TopologyComponent,Grouping> childOpList=children.get(stream);
        if(childOpList==null){
            childOpList=new HashMap<>();
        }
        childOpList.put(downOp,g);//update children
        children.put(stream,childOpList);
    }

    @Override
    public void setParents(TopologyComponent upOp, Grouping g) {
        String stream = g.getStreamID();
        Map<TopologyComponent, Grouping> parentOpList = parents.get(stream);
        if (parentOpList == null) {
            parentOpList = new HashMap<>();
        }
        parentOpList.put(upOp, g);//update parents
        parents.put(stream, parentOpList);
    }
    @Override
    public Set<String> getOutput_streamsIds() {
        return output_streams.keySet();
    }
    public Set<String> get_childrenStream() {
        return children.keySet();
    }

    public Set<String> get_parentsStream() {
        return parents.keySet();
    }
    @Override
    public Map<TopologyComponent, Grouping> getChildrenOfStream(String streamId) {
        return children.get(streamId);
    }

    @Override
    public Map<TopologyComponent, Grouping> getParentsOfStream(String streamId) {
        return parents.get(streamId);
    }

    @Override
    public HashMap<String, Map<TopologyComponent, Grouping>> getParents() {
        return parents;
    }

    @Override
    public boolean isLeafNode() {
        return children.isEmpty();
    }
    @Override
    public boolean isLeadNode() {
        return parents.isEmpty() && !this.children.isEmpty();
    }
}
