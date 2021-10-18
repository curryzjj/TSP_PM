package applications.bolts.wordcount;

import System.util.Configuration;
import System.util.DataTypes.StreamValues;
import System.util.OsUtils;
import UserApplications.constants.WordCountConstants.Field;
import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.MapBolt;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;


public class WordCountBolt extends MapBolt {
    private static final Logger LOG= LoggerFactory.getLogger(WordCountBolt.class);
    private final Map<Integer, Long> counts = new HashMap<>();//what if memory is not enough to hold counts?
    public WordCountBolt(){
        super(LOG);
        this.setStateful();
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes=conf.getInt("num_socket",1);
        if(numNodes==8){
            return 80;
        }else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid= OsUtils.getPID(TopologyContext.HPCMonotor);
    }

    @Override
    protected Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) throws DatabaseException, InterruptedException {
        //some process
        char[] word = input.getCharArray(0);
        int key = Arrays.hashCode(word);
        long v = counts.getOrDefault(key, 0L);
        if (v == 0) {
            counts.put(key, 1L);
            collector.force_emit(0, new StreamValues(word, 1L));
        } else {
            long value = v + 1L;
            counts.put(key, value);
            collector.force_emit(0, new StreamValues(word, value));
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
}
