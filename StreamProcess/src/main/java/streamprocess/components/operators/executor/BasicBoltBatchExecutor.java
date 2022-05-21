package streamprocess.components.operators.executor;

import engine.Exception.DatabaseException;
import engine.shapshot.SnapshotResult;
import streamprocess.components.operators.api.AbstractBolt;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public class BasicBoltBatchExecutor extends BoltExecutor{
    private final AbstractBolt _op;

    public BasicBoltBatchExecutor(AbstractBolt op) {
        super(op);
        _op = op;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }

    @Override
    public void cleanup() {
        _op.cleanup();
    }

    public void callback(int callee, Marker marker) {
        _op.callback(callee, marker);
    }

    @Override
    public void ackCommit(long offset) {
        this._op.cleanEpoch(offset);
    }

    @Override
    public void recoveryInput(long offset, List<Integer> recoveryExecutorIDs) throws FileNotFoundException, InterruptedException {

    }


    public void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        try {
            _op.execute(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        try {
            _op.execute(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void profile_execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        try {
            _op.profile_execute(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
