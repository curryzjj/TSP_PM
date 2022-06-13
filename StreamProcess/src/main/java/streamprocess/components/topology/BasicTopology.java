package streamprocess.components.topology;

import System.constants.BaseConstants;
import System.spout.helper.parser.Parser;
import System.util.ClassLoaderUtils;
import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.components.operators.base.BaseSink;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 */
public abstract class BasicTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BasicTopology.class);
    protected final int spoutThreads;
    protected final int sinkThreads;
    protected AbstractSpout spout;
    protected BaseSink sink;
    protected Parser parser;
    protected BasicTopology(String topologyName, Configuration config){
        super(topologyName,config);
        boolean profile = config.getBoolean("profile");
        boolean benchmark = config.getBoolean("benchmark");
        if (!benchmark) {
            spoutThreads = config.getInt("spoutThread", 1);//now read from parameters.
            sinkThreads = config.getInt( "sinkThread", 1);
        } else {
            spoutThreads = 1;
            sinkThreads = 1;
        }
    }
    protected void initilize_parser(){
        String parserClass = config.getString(getConfigKey(), null);
        if (parserClass != null) {

            parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);

            parser.initialize(config);
        } else LOG.info("No parser is initialized");
    }//not sure

    @Override
    public void initialize() {
        config.setConfigPrefix(getConfigPrefix());
        spout=loadSpout();
        sink=loadSink();
        sink.setConfigPrefix(getConfigPrefix());
        spout.setConfigPrefix(getConfigPrefix());
        initilize_parser();
    }
}
