package streamprocess.components.operators.base.transaction;

import engine.Exception.DatabaseException;
import engine.shapshot.SnapshotResult;
import engine.transaction.impl.TxnManagerTStream;
import org.slf4j.Logger;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.operators.api.TransactionalBolt;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public abstract class TransactionalBoltTStream extends TransactionalBolt {
    public TransactionalBoltTStream(Logger log,int fid){
        super(log,fid);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager=new TxnManagerTStream(db.getStorageManager(),this.context.getThisComponentId(),thread_Id,NUM_SEGMENTS,this.context.getThisComponent().getNumTasks());
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector){
        loadDB(context.getThisTaskId()-context.getThisComponent().getExecutorList().get(0).getExecutorID(),context.getThisTaskId(),context.getGraph());
    }
    //used in the T-Stream_CC
    protected abstract void PRE_TXN_PROCESS(Tuple in) throws DatabaseException, InterruptedException;
    protected abstract void TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException;
    protected void REQUEST_POST() throws InterruptedException{};//implement in the application
    protected void REQUEST_REQUEST_CORE() throws InterruptedException{};//implement in the application
    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        //pre stream processing phase..
        if(status.isMarkerArrived(in.getSourceTask())){
            PRE_EXECUTE(in);
        }else{
            PRE_TXN_PROCESS(in);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                forward_checkpoint(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                this.collector.ack(in,in.getMarker());
                if(in.getMarker().getValue()=="recovery"){
                    this.registerRecovery();
                }else{
                    this.needcheckpoint=true;
                    this.checkpointId=checkpointId;
                    TXN_PROCESS();
                }
            }
        }else{
            execute_ts_normal(in);
        }
    }
}
