package streamprocess.components.topology;

import System.constants.BaseConstants;
import System.util.ClassLoaderUtils;
import System.util.Configuration;
import com.sun.org.apache.xerces.internal.util.SynchronizedSymbolTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.components.operators.base.BaseSink;
import sun.misc.ClassLoaderUtil;

/**
 * Extended by the user to implement their topology-logic
 */
public abstract class AbstractTopology  {
    private static final Logger LOG= LoggerFactory.getLogger(AbstractTopology.class);
    protected final TopologyBuilder builder;
    protected final Configuration config;
    private final String topologyName;
    protected AbstractTopology(String topologyName, Configuration config){
        this.topologyName=topologyName;
        this.config=config;
        this.builder=new TopologyBuilder();
    }
    //the below function are used in the log
    protected String getConfigKey(){
        return String.format(BaseConstants.BaseConf.SPOUT_PARSER, getConfigPrefix());
    }
    protected String getConfigKey(String name){
        return String.format(BaseConstants.BaseConf.SINK_THREADS, String.format("%s.%s", getConfigPrefix(), name));
    }
    //end
    public String getTopologyName() {
        return topologyName;
    }
    //loadSpout and some overload function
    AbstractSpout loadSpout(){
        return loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, getConfigPrefix());
    }
    protected AbstractSpout loadSpout(String configKey, String configPrefix){
        String spoutClass=config.getString(String.format(configKey,configPrefix));
        AbstractSpout spout;
        spout=(AbstractSpout) ClassLoaderUtils.newInstance(spoutClass,"spout",getLogger());
        spout.setConfigPrefix(configPrefix);
        return spout;
    }
    //end
    //loadSink and some overload function
    BaseSink loadSink(){
        return loadSink(BaseConstants.BaseConf.SINK_CLASS, getConfigPrefix());
    }
    protected BaseSink loadSink(String configKey, String configPrefix){
        String sinkClass = config.getString(String.format(configKey, configPrefix));
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.setConfigPrefix(configPrefix);
        return sink;
    }
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
