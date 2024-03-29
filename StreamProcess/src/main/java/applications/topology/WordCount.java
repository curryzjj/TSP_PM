package applications.topology;

import System.util.Configuration;
import UserApplications.constants.WordCountConstants.Field;
import UserApplications.constants.WordCountConstants.Component;
import applications.bolts.common.StringParserBolt;
import applications.bolts.wordcount.SplitSentenceBolt;
import applications.bolts.wordcount.WordCountBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.topology.BasicTopology;
import streamprocess.components.topology.Topology;
import streamprocess.controller.input.InputStreamController;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.execution.runtime.tuple.Fields;

import static UserApplications.constants.WordCountConstants.PREFIX;

public class WordCount extends BasicTopology{
    private static final Logger LOG= LoggerFactory.getLogger(WordCount.class);
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
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT,spout,spoutThreads);
            StringParserBolt parserBolt=new StringParserBolt(parser,new Fields(Field.WORD));
            builder.setBolt(Component.PARSER,parserBolt,2,new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.SPLITTER,new SplitSentenceBolt(),2,new ShuffleGrouping(Component.PARSER));
            builder.setBolt(Component.COUNTER,new WordCountBolt(),2,new ShuffleGrouping(Component.SPLITTER));
            builder.setSink(Component.SINK,sink,sinkThreads,new ShuffleGrouping(Component.COUNTER));

        }catch (InvalidIDException e){
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler() {
        });
        return builder.createTopology();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected String getConfigPrefix() {
        return PREFIX;
    }
}
