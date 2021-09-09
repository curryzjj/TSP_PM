package streamprocess.components.operators.executor;

import System.util.Configuration;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Marker;

import java.util.Map;

public class BasicSpoutBatchExecutor extends SpoutExecutor{
    private final AbstractSpout _op;

    BasicSpoutBatchExecutor(AbstractSpout op) {
        super(op);
        this._op=op;
    }

    public void bulk_emit_nonblocking(int batch) throws InterruptedException {
        for (int i = 0; i < batch; i++) {
            _op.nextTuple_noblocking();
        }
    }
    public void bulk_emit(int batch) throws InterruptedException {
        for (int i = 0; i < batch; i++) {
            _op.nextTuple();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public int getID() {
        return 0;
    }

    @Override
    public double get_read_selectivity() {
        return 0;
    }

    @Override
    public Map<String, Double> get_input_selectivity() {
        return null;
    }

    @Override
    public Map<String, Double> get_output_selectivity() {
        return null;
    }

    @Override
    public double get_branch_selectivity() {
        return 0;
    }

    @Override
    public String getConfigPrefix() {
        return null;
    }

    @Override
    public TopologyContext getContext() {
        return null;
    }

    @Override
    public void display() {

    }

    @Override
    public double getResults() {
        return 0;
    }

    @Override
    public double getLoops() {
        return 0;
    }

    @Override
    public boolean isScalable() {
        return false;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void callback(int callee, Marker marker) {

    }

    @Override
    public Integer default_scale(Configuration conf) {
        return null;
    }
}
