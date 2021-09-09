package streamprocess.components.topology;

import System.util.Configuration;
import org.apache.log4j.Logger;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.components.operators.base.BaseSink;
/**
 * Extended by the user to implement their logic
 */
public abstract class AbstractTopology  {
    protected final TopologyBuilder builder;
    protected final Configuration config;
    private final String topologyName;
    protected AbstractTopology(String topologyName, Configuration config){
        this.topologyName=topologyName;
        this.config=config;
        this.builder=new TopologyBuilder();
    }
    protected String getConfigKey(){ return null;}
    protected String getConfigKey(String name){ return null;}
    public String getTopologyName() {
        return topologyName;
    }
    //loadSpout and some overload function
    AbstractSpout loadSpout(){return null;}
    protected AbstractSpout loadSpout(String configKey, String configPrefix){return null;}
    protected AbstractSpout loadSpout(String name){return null;}
    //end
    //loadSink and some overload function
    BaseSink loadSink(){return null;}
    protected BaseSink loadSink(String configKey, String configPrefix){return null;}
    protected BaseSink loadSink(String name){return null;}
    //end
    /**
     * Utility method to parse a configuration key with the application prefix..
     *
     * @return
     */
    public abstract void initialize();
    public abstract Topology buildTopology();
    protected abstract Logger getLogger();
    protected abstract String getConfigPrefix();
}
