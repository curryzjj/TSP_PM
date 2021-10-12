package streamprocess.components.operators.api;

import org.slf4j.Logger;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public abstract class AbstractBolt extends Operator {

    private AbstractBolt(Logger log, boolean byP, double event_frequency, double w) {
        super(log, byP, event_frequency, w);
    }
    AbstractBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double branch_selectivity
            , double read_selectivity, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, false, event_frequency, w);
    }
    protected AbstractBolt(Logger log, Map<String, Double> input_selectivity,
                           Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, 1, 1, byP, event_frequency, w);
    }
    public abstract void execute(Tuple in) throws InterruptedException, Exception, BrokenBarrierException;//shoud be the DatabaseException
    public void execute(JumboTuple in) throws InterruptedException, Exception, BrokenBarrierException {}

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void callback(int callee, Marker marker) {
        super.callback(callee, marker);
    }
    public void profile_execute(JumboTuple in) throws InterruptedException, Exception, BrokenBarrierException {//shoud be the DatabaseException
        execute(in);
    }
}
