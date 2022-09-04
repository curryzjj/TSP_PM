package streamprocess.components.operators.executor;

import System.util.Configuration;
import engine.Clock;
import engine.Exception.DatabaseException;
import streamprocess.components.operators.api.Operator;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.clr.CausalService;
import streamprocess.faulttolerance.clr.RecoveryDependency;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BoltExecutor implements IExecutor {
    private static final long serialVersionUID = -462759707942430500L;
    private final Operator op;

    BoltExecutor(Operator op) {
        this.op = op;
    }
    //implemented by the BatchExecutor  (DatatypeException->DatabaseException)
    public abstract void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    public abstract void profile_execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    //end
    public void setExecutionNode(ExecutionNode e){ op.setExecutionNode(e); }
    //public void setclock(Clock clock){}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        op.prepare(stormConf,context,collector);
    }
    public void loadDB(Configuration conf, TopologyContext context, OutputCollector collector){
        op.loadDB(context);
    }
    @Override
    public int getID() { return op.getId(); }
    @Override
    public String getConfigPrefix() {
        return op.getConfigPrefix();
    }

    @Override
    public TopologyContext getContext() {
        return op.getContext();
    }

    @Override
    public void display() {
        op.display();
    }
    @Override
    public void recoveryInput(long offset, List<Integer> recoveryExecutorIDs, long alignOffset) throws FileNotFoundException, InterruptedException {

    }

    @Override
    public void ackCommit(boolean isRecovery, long alignMarkerId) {
        if (isRecovery) {
            this.op.setRecoveryId(alignMarkerId);
        }
        this.op.isCommit =true;
    }
    @Override
    public void ackCommit(long offset) {
        this.op.cleanEpoch(offset);
    }

    @Override
    public RecoveryDependency ackRecoveryDependency() {
        return this.op.returnRecoveryDependency();
    }
    @Override
    public ConcurrentHashMap<Integer, CausalService> ackCausalService() {
        return this.op.returnCausalService();
    }

    @Override
    public int getStage() {
        return op.getFid();
    }

    @Override
    public boolean IsStateful() {
        return op.IsStateful();
    }

    @Override
    public double getEmpty() {
        return 0;
    }

    public void setclock(Clock clock) {
        this.op.clock=clock;
    }

}
