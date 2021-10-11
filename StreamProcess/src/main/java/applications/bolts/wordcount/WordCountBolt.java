package applications.bolts.wordcount;

import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.MapBolt;
import streamprocess.components.topology.TopologyContext;
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
