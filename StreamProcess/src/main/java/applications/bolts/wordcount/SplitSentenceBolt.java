package applications.bolts.wordcount;

import System.constants.BaseConstants;
import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.splitBolt;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;

public class SplitSentenceBolt extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt.class);
    public SplitSentenceBolt() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 10.0);
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return super.default_scale(conf);
    }

    @Override
    public void execute(Tuple in) throws Exception {
        //some process
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
