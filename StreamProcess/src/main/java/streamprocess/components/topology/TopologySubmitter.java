package streamprocess.components.topology;

import System.util.Configuration;
import net.openhft.affinity.AffinitySupport;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.affinity.SequentialBinding;
import streamprocess.optimization.OptimizationManager;

import java.io.IOException;
import java.util.Collection;

import static UserApplications.CONTROL.enable_shared_state;
import static streamprocess.execution.affinity.SequentialBinding.SequentialBindingDB;

public class TopologySubmitter {
    private final static Logger LOG= LoggerFactory.getLogger(TopologySubmitter.class);
    private OptimizationManager OM;
    public OptimizationManager getOM() {
        return OM;
    }
    public void setOM(OptimizationManager OM) {
        this.OM = OM;
    }

    public Topology submitTopology(Topology topology, Configuration conf) throws UnhandledCaseException, IOException {
        //compile
        ExecutionGraph g=new TopologyCompiler().generateEG(topology,conf);
        g.display_ExecutionNodeList();
        Collection<TopologyComponent> topologyComponents=g.topology.getRecords().values();
        //TODO:Some Metrics code
        //launch
        OM = new OptimizationManager(g,conf,conf.getBoolean("profile",false),conf.getDouble("relax",1),topology.getPlatform());
        if(enable_shared_state){
            LOG.info("DB initialize starts @" + DateTime.now());
            long start = System.nanoTime();
            SequentialBindingDB();
            //TODO:implement spinlock, used in the partition
            g.topology.tableinitilizer = topology.txnTopology.createDB(null);
            long end = System.nanoTime();
            LOG.info("DB initialize takes:" + (end - start) / 1E6 + " ms");
            OM.launch(g.topology,topology.getPlatform(),topology.db);
        }else{
            OM.launch(topology,topology.getPlatform(),topology.db);
        }
        //start
        OM.start();
        return  g.topology;
    }
}
