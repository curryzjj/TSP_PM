package streamprocess.execution.runtime.collector;

import streamprocess.components.topology.TopologyComponent;

import java.util.HashMap;

public class MetaGroup {
    private final int taskId;
    HashMap<TopologyComponent,Meta> map=new HashMap<>();
    public MetaGroup(int taskId){
        this.taskId=taskId;
    }
    public Meta get(TopologyComponent childOP){
        return map.get(childOP);
    }
    public void put(TopologyComponent childrenOp,Meta meta){
        map.put(childrenOp,meta);
    }
}
