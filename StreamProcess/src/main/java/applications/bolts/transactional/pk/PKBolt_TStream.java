package applications.bolts.transactional.pk;

import System.parser.SensorParser;
import applications.events.PKEvent;
import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.transaction.TransactionalBoltTStream;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.ArrayDeque;
import java.util.Random;

import static UserApplications.constants.PKConstants.Constant.SIZE_EVENT;
import static UserApplications.constants.PKConstants.Constant.SIZE_VALUE;

public class PKBolt_TStream extends TransactionalBoltTStream {
    private static final Logger LOG= LoggerFactory.getLogger(PKBolt_TStream.class);
    final SensorParser parser=new SensorParser();
    private final ArrayDeque<PKEvent> PKEvents = new ArrayDeque<>();
    Random r = new Random();
    private double[][] value;//value[40][50]
    public PKBolt_TStream(int fid){
        super(LOG,fid);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        value=new double[SIZE_EVENT][];
        for (int i = 0; i < SIZE_EVENT; i++) {
            value[i] = new double[SIZE_VALUE];
            for (int j = 0; j < SIZE_VALUE; j++) {
                value[i][j] = r.nextDouble() * 100;
            }
        }
    }

    @Override
    protected void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException {

    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {

    }


    @Override
    protected void TXN_PROCESS() throws DatabaseException, InterruptedException {

    }
}
