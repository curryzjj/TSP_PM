package streamprocess.components.operators.api;

import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public abstract class AbstractBolt extends Operator {

    private AbstractBolt(Logger log, boolean byP, double event_frequency, double w) {
        super(log, byP, event_frequency, w);
    }
    protected AbstractBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double branch_selectivity
            , double read_selectivity, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, false, event_frequency, w);
    }
    protected AbstractBolt(Logger log, Map<String, Double> input_selectivity,
                           Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, 1, 1, byP, event_frequency, w);
    }
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException;//shoud be the DatabaseException
    public void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {}

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void callback(int callee, Marker marker) {
       status.callback_bolt(callee,marker,executor);
    }
    public void profile_execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {//shoud be the DatabaseException
        execute(in);
    }
}
