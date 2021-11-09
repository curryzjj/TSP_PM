package applications.sink;

import System.sink.helper.stable_sink_helper;
import System.util.Configuration;
import System.util.OsUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.base.BaseSink;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Tuple;

import java.util.ArrayDeque;
import java.util.HashMap;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private static final DescriptiveStatistics latency = new DescriptiveStatistics();

    private static final long serialVersionUID = 6249684803036342603L;
    protected static String directory;
    protected static String algorithm;
    protected static boolean profile = false;
    protected stable_sink_helper helper;
    protected int ccOption;
    private boolean LAST = false;
    private int exe;
    protected final ArrayDeque<Long> latency_map = new ArrayDeque();
    public int batch_number_per_wm;

    public MeasureSink() {
        super(new HashMap<>());
        this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put("tn", 1.0);
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        return 1;
    }

    public void initialize(int task_Id_InGroup, int thisTaskId, ExecutionGraph graph) {
        super.initialize(task_Id_InGroup, thisTaskId, graph);
        batch_number_per_wm = 100;
        exe = NUM_EVENTS;
        LOG.info("expected last events = " + exe);
    }

    protected int tthread;
    int cnt = 0;
    long meauseStartTime;
    long meauseLatencyStartTime;
    long meauseFinishTime;
    long latencyCount=0;

    @Override
    public void execute(Tuple input) throws InterruptedException {
        if(enable_latency_measurement){
            //this.latency_measure();
        }
    }
    protected void latency_measure() {
            if (cnt == 0) {
                meauseLatencyStartTime = System.nanoTime();
            } else {
                if (cnt % batch_number_per_wm == 0) {
                    latencyCount++;
                    final long end = System.nanoTime();
                    final long process_latency = end - meauseLatencyStartTime;//ns
                    latency_map.add(process_latency / batch_number_per_wm);
                    meauseLatencyStartTime = end;
                }
            }
            cnt++;
    }

    protected void measure_end() {
        long time_elapsed = meauseFinishTime - meauseStartTime;
        double throughtResult= ((double) (cnt) * 1E6 / time_elapsed);//count/ns * 1E6 --> EVENTS/ms
        long latencySum=0;
        for (Long entry : latency_map) {
            latencySum=latencySum+entry;
        }
        double avg_latency=((double) (latencySum)  /latencyCount );
        LOG.info(this.executor.getOP_full()+"\tReceived:" + cnt+" events" + " Throughput(k input_event/s) of:\t" + throughtResult);
        LOG.info(this.executor.getOP_full()+"\tAverage latency(ms) of:\t" + avg_latency);
        if (thisTaskId == graph.getSink().getExecutorID()) {
            LOG.info("Thread:" + thisTaskId + " is going to stop all threads sequentially");
            context.Sequential_stopAll();
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
