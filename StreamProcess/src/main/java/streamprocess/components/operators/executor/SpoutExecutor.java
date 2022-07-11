package streamprocess.components.operators.executor;

import engine.Clock;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.emitMarker;
import streamprocess.components.operators.api.Operator;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.tuple.msgs.Marker;

public abstract class SpoutExecutor implements IExecutor {
    private final Operator op;
    SpoutExecutor(Operator op){this.op=op;}

    @Override
    public void setExecutionNode(ExecutionNode e) {
        op.setExecutionNode(e);
    }
    //OLTP
    //public void configureWriter(){}
    //public void configureLocker(){}
    public void loadDB(TopologyContext context){
        op.loadDB(context);
    }
    public void setclock(Clock clock) {
        this.op.clock=clock;
    }

    public int getStage() {
        return op.getFid();
    }

    @Override
    public void ackSignal(Tuple message) {
        ((emitMarker) op).ack_Signal(message);
    }

    @Override
    public void earlier_clean_state(Marker marker) {

    }

    public boolean IsStateful() {
        return op.IsStateful();
    }

    public void forceStop() {
        op.forceStop();
    }



    public double getEmpty() {
        return op.getEmpty();
    }
}
