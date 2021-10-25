package applications.bolts.common;

import System.spout.helper.parser.Parser;
import System.util.Configuration;
import System.parser.StringParser;
import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.MapBolt;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.concurrent.BrokenBarrierException;

public class StringParserBolt extends MapBolt {

    final StringParser parser;
    private final Fields fields;
    private static final Logger LOG = LoggerFactory.getLogger(StringParserBolt.class);

    public StringParserBolt(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (StringParser) parser;
        this.fields = fields;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 4;
        } else {
            return 1;
        }
    }

    @Override
    protected Fields getDefaultFields() {
        return fields;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException{
        //some process on the Tuple in
        char[] string=in.getCharArray(0);
        char[] emit=parser.parse(string);
        collector.force_emit(emit);
    }

    @Override
    public void execute(JumboTuple in) throws DatabaseException, BrokenBarrierException, InterruptedException {
        super.execute(in);
    }

    @Override
    public void profile_execute(JumboTuple in) throws DatabaseException, BrokenBarrierException, InterruptedException {
        super.profile_execute(in);
    }
}
