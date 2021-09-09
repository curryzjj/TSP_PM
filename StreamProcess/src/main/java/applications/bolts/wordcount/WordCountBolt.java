package applications.bolts.wordcount;

import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.MapBolt;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.concurrent.BrokenBarrierException;


public class WordCountBolt extends MapBolt {
    private static final Logger LOG= LoggerFactory.getLogger(WordCountBolt.class);
    public WordCountBolt(){
        super(LOG);
        this.setStateful();
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return super.default_scale(conf);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
    }

    @Override
    protected Fields getDefaultFields() {
        return super.getDefaultFields();
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
