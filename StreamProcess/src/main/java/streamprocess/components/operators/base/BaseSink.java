package streamprocess.components.operators.base;

import System.constants.BaseConstants;
import System.util.ClassLoaderUtils;
import System.util.Configuration;
import applications.sink.formatter.BasicFormatter;
import applications.sink.formatter.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.faulttolerance.checkpoint.emitMarker;
import streamprocess.faulttolerance.clr.CausalService;
import streamprocess.faulttolerance.clr.RecoveryDependency;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

import static UserApplications.CONTROL.*;

public abstract class BaseSink extends BaseOperator implements emitMarker {
    private static final Logger LOG= LoggerFactory.getLogger(BaseSink.class);
    protected static ExecutionGraph graph;
    protected int thisTaskId;
    boolean isSINK=true;
    protected static final int max_num_msg=(int) 1E5;
    //<MarkerId,RD>
    public ConcurrentHashMap<Long, RecoveryDependency> recoveryDependency = new ConcurrentHashMap<>();
    public long currentMarkerId = 0;
    //<PartitionId, CausalService>
    public ConcurrentHashMap<Integer, CausalService> causalService = new ConcurrentHashMap<>();
    //<UpstreamId,bufferQueue>
    public HashMap<Integer, Queue<Tuple>> bufferedTuples = new HashMap<>();
    private BaseSink(Logger log){super(log);}
    BaseSink(Map<String, Double> input_selectivity, double read_selectivity) {
        super(LOG, input_selectivity, null, (double) 1, read_selectivity);
    }
    public BaseSink(Map<String, Double> input_selectivity) {
        this(input_selectivity, 0);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thisTaskId=thisTaskId;
        BaseSink.graph=graph;
        String formatterClass=config.getString(getConfigKey(),null);
        Formatter formatter;
        //TODO:initialize formatter
        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }

        formatter.initialize(Configuration.fromMap(config), getContext());
        if(thisTaskId == graph.getSink().getExecutorID()){
            isSINK = true;
        }
        for (String stream:this.executor.operator.getParents().keySet()) {
            for (TopologyComponent topologyComponent:this.executor.operator.getParentsOfStream(stream).keySet()) {
                for (int id:topologyComponent.getExecutorIDList()) {
                    this.bufferedTuples.put(id, new ArrayDeque<>());
                }
            }
        }
        if (enable_determinants_log) {
            for (int i = 0; i < config.getInt("partition_num"); i++) {
                this.causalService.put(i, new CausalService());
            }
        }
    }

    public void addRecoveryDependency(long markerId) {
        if(!this.recoveryDependency.containsKey(markerId)) {
            RecoveryDependency RD;
            if (recoveryDependency.get(currentMarkerId) != null) {
                RD = new RecoveryDependency(this.recoveryDependency.get(currentMarkerId).getRecoveryDependency(),markerId);
            } else {
                RD = new RecoveryDependency(PARTITION_NUM, markerId);
            }
            this.recoveryDependency.put(markerId,RD);
        }
    }

    @Override
    public void cleanEpoch(long offset) {
        if (enable_recovery_dependency) {
            RecoveryDependency RD = new RecoveryDependency(PARTITION_NUM, this.currentMarkerId);
            this.recoveryDependency.put(currentMarkerId, RD);
        } else if (enable_determinants_log) {
           for (CausalService causalService:this.causalService.values()) {
               causalService.cleanDeterminant();
           }
        }
        if (Exactly_Once) {
            twoPC_CommitTuple(offset);
        }
    }

    @Override
    public RecoveryDependency returnRecoveryDependency() {
        this.status.source_status_ini(this.executor);
        return this.recoveryDependency.get(currentMarkerId);
    }

    @Override
    public ConcurrentHashMap<Integer, CausalService> returnCausalService() {
        this.status.source_status_ini(this.executor);
        return this.causalService;
    }

    protected abstract Logger getLogger();
    protected abstract void PRE_EXECUTE(Tuple in);
    protected abstract void execute_ts_normal(Tuple in);
    protected abstract void EXECUTE(Tuple in);
    protected abstract void twoPC_CommitTuple(long bid);
    protected abstract void BUFFER_EXECUTE() throws IOException, InterruptedException;
    @Override
    protected Fields getDefaultFields() {
        return new Fields("");
    }
    private String getConfigKey() {
        return String.format(BaseConstants.BaseConf.SINK_FORMATTER, configPrefix);
    }
    protected void killTopology(){
        LOG.info("Killing application");
    }
    @Override
    public boolean marker() throws InterruptedException, BrokenBarrierException {
        return false;
    }

    @Override
    public void forward_marker(int sourceId, long bid, Marker marker, String msg) throws InterruptedException {
    }

    @Override
    public void forward_marker(int sourceTask, String streamId, long bid, Marker marker, String msg) throws InterruptedException {
    }

    @Override
    public void ack_Signal(Tuple message) {
    }

    @Override
    public void earlier_ack_marker(Marker marker) {
    }
}
