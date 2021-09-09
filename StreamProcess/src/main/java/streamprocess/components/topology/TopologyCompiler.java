package streamprocess.components.topology;

import System.util.Configuration;
import streamprocess.execution.ExecutionGraph;

public class TopologyCompiler {
    public ExecutionGraph generateEG(Topology topology, Configuration conf){
        ExecutionGraph executionGraph = new ExecutionGraph(topology, null, conf);
        return executionGraph;
    }
}

