package streamprocess.components.topology;

import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.execution.ExecutionGraph;
import streamprocess.optimization.OptimizationManager;

import java.util.Collection;

import static UserApplications.CONTROL.enable_shared_state;

public class TopologySubmitter {
    private final static Logger LOG= LoggerFactory.getLogger(TopologySubmitter.class);
    private OptimizationManager OM;
    public OptimizationManager getOM() {
        return OM;
    }
    public void setOM(OptimizationManager OM) {
        this.OM = OM;
    }

    public Topology submitTopology(Topology topology, Configuration conf) throws UnhandledCaseException {
        //compile
        ExecutionGraph g=new TopologyCompiler().generateEG(topology,conf);
        g.display_ExecutionNodeList();
        Collection<TopologyComponent> topologyComponents=g.topology.getRecords().values();
        //TODO:Some Metrics code
        //launch
        OM=new OptimizationManager(g,conf,conf.getBoolean("profile",false),conf.getDouble("relax",1),topology.getPlatform());
        if(enable_shared_state){
            /**
             * TODO:binding the DB to CPU
             * TODO:OM.lanuch
             */
        }else{
            OM.launch(topology,topology.getPlatform(),topology.db);
        }
        //start
        OM.start();
        return  g.topology;
    }
}
