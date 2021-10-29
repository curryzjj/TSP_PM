package streamprocess.components.operators.base.transaction;

import engine.Exception.DatabaseException;
import engine.transaction.impl.TxnManagerTStream;
import org.slf4j.Logger;
import streamprocess.components.operators.api.TransactionalBolt;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public abstract class TransactionalBoltTStream extends TransactionalBolt {
    public TransactionalBoltTStream(Logger log,int fid){
        super(log,fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        //TODO:initialize the transactionManager
        transactionManager=new TxnManagerTStream(db.getStorageManager(),this.context.getThisComponentId(),thread_Id,NUM_SEGMENTS,this.context.getThisComponent().getNumTasks());
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector){
        loadDB(context.getThisTaskId()-context.getThisComponent().getExecutorList().get(0).getExecutorID(),context.getThisTaskId(),context.getGraph());
    }
    //used in the T-Stream_CC
    protected abstract void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException;
    protected abstract void TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException;
    protected void REQUEST_POST() throws InterruptedException{};//implement in the application
    protected void REQUEST_REQUEST_CORE() throws InterruptedException{};//implement in the application
    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        //pre stream processing phase..
        PRE_EXECUTE(in);
        PRE_TXN_PROCESS(_bid, timestamp);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if(in.isMarker()){
            TXN_PROCESS();
        }else{
            execute_ts_normal(in);
        }
    }
}
