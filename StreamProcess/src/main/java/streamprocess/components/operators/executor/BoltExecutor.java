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

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public abstract class BoltExecutor implements IExecutor {
    private final Operator op;

    BoltExecutor(Operator op) {
        this.op = op;
    }
    //implemented by the BatchExecutor  (DatatypeException->DatabaseException)
    public abstract void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    public abstract void profile_execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    //end

    public void loadDB(Configuration conf,TopologyContext context,OutputCollector collector){
        op.loadDB(conf,context,collector);
    }
    public  void setExecutionNode(ExecutionNode e){ op.setExecutionNode(e); }
    //public void setclock(Clock clock){}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        op.prepare(stormConf,context,collector);
    }
    @Override
    public int getID() { return op.getId(); }
    @Override
    public double get_read_selectivity() { return op.read_selectivity; }

    @Override
    public Map<String, Double> get_input_selectivity() {
        return op.input_selectivity;
    }

    @Override
    public Map<String, Double> get_output_selectivity() {
        return op.output_selectivity;
    }

    @Override
    public double get_branch_selectivity() {
        return op.branch_selectivity;
    }

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
    public double getResults() {
        return op.getResults();
    }

    @Override
    public double getLoops() {
        return op.loops;
    }

    @Override
    public boolean isScalable() {
        return op.scalable;
    }
    @Override
    public Integer default_scale(Configuration conf) {
        return op.default_scale(conf);
    }
    //public void configureWriter(Writer writer) { }

    //public void configureLocker(OrderLock lock, OrderValidate orderValidate) { }

    @Override
    public void earlier_clean_state(Marker marker) { }

    @Override
    public void clean_status(Marker marker) {

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
    public void forceStop() {
        op.forceStop();
    }

    @Override
    public double getEmpty() {
        return 0;
    }

    public void setclock(Clock clock) {
        this.op.clock=clock;
    }
}
