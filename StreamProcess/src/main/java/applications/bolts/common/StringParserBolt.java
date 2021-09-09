package applications.bolts.common;

import System.spout.helper.parser.Parser;
import System.util.Configuration;
import UserApplications.parser.StringParser;
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
        return super.default_scale(conf);
    }

    @Override
    protected Fields getDefaultFields() {
        return super.getDefaultFields();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, Exception, BrokenBarrierException {
        //some process on the Tuple in
    }

    @Override
    public void execute(JumboTuple in) throws Exception {
        super.execute(in);
    }

    @Override
    public void profile_execute(JumboTuple in) throws Exception {
        super.profile_execute(in);
    }
}
