package applications.topology;

import System.util.Configuration;
import UserApplications.constants.WordCountConstants.Component;
import applications.bolts.common.StringParserBolt;
import applications.bolts.wordcount.SplitSentenceBolt;
import applications.bolts.wordcount.WordCountBolt;
import org.apache.log4j.Logger;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.topology.BasicTopology;
import streamprocess.components.topology.Topology;
import streamprocess.controller.input.InputStreamController;
import streamprocess.execution.runtime.tuple.Fields;

public class WordCount extends BasicTopology{
    public WordCount(String topologyName, Configuration config){
        super(topologyName,config);
    }

    @Override
    public void initialize() {
        super.initialize();
    }

    @Override
    public Topology buildTopology() {
        try{
            spout.setFields(new Fields(""));
            builder.setSpout(Component.SPOUT,spout,spoutThreads);
            StringParserBolt parserBolt=new StringParserBolt(parser,new Fields(""));
            builder.setBolt(Component.PARSER,parserBolt,1,new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.SPLITTER,new SplitSentenceBolt(),1,new ShuffleGrouping(Component.PARSER));
            builder.setBolt(Component.COUNTER,new WordCountBolt(),1,new ShuffleGrouping(Component.SPLITTER));
            builder.setSink(Component.SINK,sink,sinkThreads,new ShuffleGrouping(Component.COUNTER));

        }catch (InvalidIDException e){
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new InputStreamController() {
        });
        return builder.createTopology();
    }

    @Override
    protected Logger getLogger() {
        return null;
    }

    @Override
    protected String getConfigPrefix() {
        return null;
    }
}
