package streamprocess.components.topology;

import System.util.Configuration;
import streamprocess.execution.ExecutionGraph;
import streamprocess.optimization.OptimizationManager;

public class TopologySubmitter {
    private OptimizationManager OM;

    public OptimizationManager getOM() {
        return OM;
    }

    public void setOM(OptimizationManager OM) {
        this.OM = OM;
    }

    public Topology submitTopology(Topology topology, Configuration conf){
        //compile
        ExecutionGraph g=new TopologyCompiler().generateEG(topology,conf);
        //launch
        //start
        return  g.topology;
    }
}
