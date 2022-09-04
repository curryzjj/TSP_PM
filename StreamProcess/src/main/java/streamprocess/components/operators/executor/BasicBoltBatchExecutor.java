package streamprocess.components.operators.executor;

import engine.Exception.DatabaseException;
import streamprocess.components.operators.api.AbstractBolt;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.clr.CausalService;
import streamprocess.faulttolerance.clr.RecoveryDependency;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

public class BasicBoltBatchExecutor extends BoltExecutor{
    private static final long serialVersionUID = 1985999225079889049L;
    private final AbstractBolt _op;

    public BasicBoltBatchExecutor(AbstractBolt op) {
        super(op);
        _op = op;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }


    public void callback(int callee, Tuple message) {
        _op.callback(callee, message);
    }

    @Override
    public void clean_status() {
        _op.clean_status();
    }

    @Override
    public RecoveryDependency ackRecoveryDependency() {
        return this._op.returnRecoveryDependency();
    }

    @Override
    public ConcurrentHashMap<Integer, CausalService> ackCausalService() {
        return this._op.returnCausalService();
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
