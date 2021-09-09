package streamprocess.components.topology;

import System.spout.helper.parser.Parser;
import System.util.Configuration;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.components.operators.base.BaseSink;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 */
public abstract class BasicTopology extends AbstractTopology {
    protected final int spoutThreads=0;
    protected final int sinkThreads=0;
    protected AbstractSpout spout;
    protected BaseSink sink;
    protected Parser parser;
    protected BasicTopology(String topologyName, Configuration config){
        super(topologyName,config);
    }
    protected void initilize_parser(){}//not sure

    @Override
    public void initialize() {
        config.setConfigPrefix(getConfigPrefix());
        spout=loadSpout();
        sink=loadSink();
        initilize_parser();
    }
}
