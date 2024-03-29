package streamprocess.components.operators.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyBuilder;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.faulttolerance.clr.CausalService;
import streamprocess.faulttolerance.clr.RecoveryDependency;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VirtualExecutor implements IExecutor{
    private static final Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);
    private static final long serialVersionUID = 6833979263182987686L;

    //AbstractBolt op;

    public VirtualExecutor() {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//		op.prepare(stormConf, context, collector);
    }

    @Override
    public int getID() {
        return -1;
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
    public void clean_status() {

    }

    @Override
    public void ackCommit(boolean isRecovery, long alignMarkerId) {

    }

    @Override
    public void ackCommit(long offset) {

    }

    @Override
    public RecoveryDependency ackRecoveryDependency() {
        return null;
    }
    @Override
    public ConcurrentHashMap<Integer, CausalService> ackCausalService() {
        return null;
    }

    @Override
    public void recoveryInput(long offset, List<Integer> recoveryExecutorIDs, long alignOffset) throws FileNotFoundException, InterruptedException {

    }


    @Override
    public int getStage() {
        return -1;
    }

    @Override
    public void callback(int callee, Tuple message) {

    }

    @Override
    public void setExecutionNode(ExecutionNode e) {

    }

    public void execute(JumboTuple in) throws InterruptedException {
        LOG.info("Should not being called.");
    }

    public boolean IsStateful() {
        return false;
    }


    public double getEmpty() {
        return 0;
    }
}
