package applications.bolts.wordcount;

import System.constants.BaseConstants;
import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.constants.WordCountConstants;
import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.splitBolt;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Fields;
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
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 10;
        } else {
            return 1;
        }
    }

    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
//		LOG.info("PID  = " + pid);
    }

    @Override
    public void execute(Tuple in) throws DatabaseException, InterruptedException {
        //some process
        char[] value = in.getCharArray(0);
        int index = 0;
        int length = value.length;
        for (int c = 0; c < length; c++) {
            if (value[c] == ' ') {//double measure_end.
                int len = c - index;
                char[] word = new char[len];
                System.arraycopy(value, index, word, 0, len);
                collector.force_emit(word);
                index = c + 1;
            }else if(c==length-1){
                int len=c-index+1;
                char[] word = new char[len];
                System.arraycopy(value, index, word, 0, len);
                collector.force_emit(word);
            }
        }
    }

    @Override
    public void execute(JumboTuple in) throws Exception {
        super.execute(in);
    }

    @Override
    public void profile_execute(JumboTuple in) throws Exception {
        super.profile_execute(in);
    }

    @Override
    protected Fields getDefaultFields() {
        return new Fields(WordCountConstants.Field.WORD);
    }
}
