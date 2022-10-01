package applications.sink;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.measure.MeasureTools;
import System.sink.helper.ApplicationResult;
import System.util.Configuration;
import UserApplications.CONTROL;
import engine.Exception.DatabaseException;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import streamprocess.components.operators.base.BaseSink;
import streamprocess.controller.output.Determinant.InsideDeterminant;
import streamprocess.controller.output.Determinant.OutsideDeterminant;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.faulttolerance.clr.CausalService;

import java.io.*;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentSkipListMap;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public class MeasureSink extends BaseSink {
    private static final long serialVersionUID = 6249684803036342603L;
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private final DescriptiveStatistics latency = new DescriptiveStatistics();
    private final ConcurrentSkipListMap<Long, ApplicationResult> results = new ConcurrentSkipListMap<>();
    //Exactly_Once
    protected final List<Double> latency_map = new ArrayList<>();
    //no_Exactly_Once
    protected final List<Double> No_Exactly_Once_latency_map = new ArrayList<>();
    private final DescriptiveStatistics waitTime = new DescriptiveStatistics();
    private final DescriptiveStatistics throughput = new DescriptiveStatistics();
    private DescriptiveStatistics twoPC_commit_time = new DescriptiveStatistics();
    private long commitStartTime;
    protected long startTime;
    private FileSystem localFS;
    protected static boolean profile = false;
    private int exe;
    private int CheckpointTimes = 0;

    protected static final List<Double> throughput_map = new ArrayList<>();
    /** <bid,timestamp> */

    //2PC
    protected HashMap<Long, Tuple2<Long,Long>> perCommitTuple = new HashMap<>();

    protected int abortTransaction=0;
    protected static long count;
    protected static long  p_count;
    //Computation latency every second
    private long computationLatency;
    public MeasureSink() {
        super(new HashMap<>());
        this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put("tn", 1.0);
        status = new Status();
        this.localFS = new LocalFileSystem();
        if(enable_measure){
            count = 0;
            p_count = 0;
        }
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        return 1;
    }

    public void initialize(int task_Id_InGroup, int thisTaskId, ExecutionGraph graph) {
        super.initialize(task_Id_InGroup, thisTaskId, graph);
        exe = NUM_EVENTS;
        LOG.info("expected last events = " + exe);
    }

    protected int tthread;

    @Override
    public void execute(Tuple in) throws InterruptedException, IOException {
        if (count == 0){
            startTime = System.nanoTime();
            this.startThroughputMeasure();
        }
        if(in.isMarker()){
            if (status.isMarkerArrived(in.getSourceTask())) {
                PRE_EXECUTE(in);
            } else {
                if (enable_recovery_dependency) {
                    addRecoveryDependency(in.getBID());
                    if (in.getMarker().getEpochInfo() != null) {
                        this.recoveryDependency.get(in.getBID()).addDependency(in.getMarker().getEpochInfo());
                    }
                }
                if(status.allMarkerArrived(in.getSourceTask(), this.executor)){
                    this.currentMarkerId = in.getBID();
                    if (enable_determinants_log) {
                        for (CausalService causalService : this.causalService.values()) {
                            causalService.setDeterminant(in.getBID());
                        }
                    }
                    switch (in.getMarker().getValue()) {
                        case "snapshot" :
                            this.FTM.sinkRegister(in.getBID());
                            CheckpointTimes ++;
                            BUFFER_EXECUTE();
                            break;
                        case "marker" :
                            if (enable_wal) {
                                this.FTM.sinkRegister(in.getBID());
                            }
                            BUFFER_EXECUTE();
                            break;
                        case "finish" :
                            if(CONTROL.Exactly_Once){
                                twoPC_CommitTuple(in.getBID());
                            }
                            SinkTimer.cancel();
                            measure_end();
                            BUFFER_EXECUTE();
                            context.stop_running();
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + in.getMarker().getValue());
                    }
                }
            }
        }else{
           execute_ts_normal(in);
        }
    }


    @Override
    public void execute(JumboTuple in) throws DatabaseException, BrokenBarrierException, InterruptedException {
        for(int i = 0;  i< in.length; i ++){
            boolean finish= (boolean) in.getMsg(i).getValue(0);
            if(!finish){
                LOG.info("The tuple ("+in.getMsg(i).getValue(1)+ ") is abort");
            }
        }
    }

    @Override
    protected void twoPC_CommitTuple(long bid) {
        if(enable_latency_measurement&&perCommitTuple.size()!=0){
            commitStartTime = System.nanoTime();
            double totalLatency = 0;
            double totalWaitTime = 0;
            long size=5000;//Latency is calculated every 'size' events
            long commitSize=0;
            Iterator<Map.Entry<Long, Tuple2<Long, Long>>> events = perCommitTuple.entrySet().iterator();
            while(events.hasNext()){
                Map.Entry<Long, Tuple2<Long, Long>> event = events.next();
                if(event.getKey()<bid){
                    final long end = System.nanoTime();
                    final double process_latency = ((end - event.getValue()._1)/1E6);//ms
                    final double wait_latency= ((end - event.getValue()._2)/1E6);//ms
                    totalLatency=totalLatency+process_latency;
                    totalWaitTime=totalWaitTime+wait_latency;
                    size--;
                    count++;
                    commitSize++;
                    events.remove();
                    if(size == 0){
                        latency_map.add(totalLatency/5000);
                        totalLatency=0;
                        size=5000;
                    }
                }
            }
            if (size != 0) {
                latency_map.add(totalLatency/(5000-size));
            }
            waitTime.addValue(totalWaitTime/commitSize);
            twoPC_commit_time.addValue((System.nanoTime()-commitStartTime)/1E6);
        }
    }

    @Override
    protected void BUFFER_EXECUTE() throws IOException, InterruptedException {
        for (Queue<Tuple> tuples : bufferedTuples.values()) {
            if (tuples.size() != 0) {
                boolean nextUpstream = false;
                while (!nextUpstream) {
                    Tuple tuple = tuples.poll();
                    if (tuple != null) {
                        execute(tuple);
                        if (tuple.isMarker()) {
                            nextUpstream = true;
                        }
                    } else {
                        nextUpstream = true;
                    }
                }
            }
        }
    }

    @Override
    protected void execute_ts_normal(Tuple in) {
        if (status.isMarkerArrived(in.getSourceTask())) {
            PRE_EXECUTE(in);
        } else {
            EXECUTE(in);
        }
    }
    @Override
    protected void PRE_EXECUTE(Tuple in) {
        bufferedTuples.get(in.getSourceTask()).add(in);
    }

    @Override
    protected void EXECUTE(Tuple in) {
        this.results.put(in.getBID(), (ApplicationResult) in.getValue(4));
        boolean finish = (boolean) in.getValue(0);
        if (!finish) {
            if (enable_determinants_log) {
                if (in.getValue(1) != null) {
                    InsideDeterminant insideDeterminant = (InsideDeterminant) in.getValue(1);
                    this.causalService.get(insideDeterminant.partitionId).addAbortEvent(insideDeterminant.input);
                }
            }
            abortTransaction++;
        } else {
            if (Exactly_Once) {
                perCommitTuple.put(in.getBID(),new Tuple2<>((long)in.getValue(2), System.nanoTime()));
            } else {
                if (enable_determinants_log) {
                    if (in.getValue(1) != null) {
                        InsideDeterminant insideDeterminant = (InsideDeterminant) in.getValue(1);
                        this.causalService.get(insideDeterminant.partitionId).addInsideDeterminant(insideDeterminant);
                    }
                    if (in.getValue(2) != null) {
                        for (int targetPartition:((OutsideDeterminant) in.getValue(2)).targetPartitionIds) {
                            this.causalService.get(targetPartition).addOutsideDeterminant((OutsideDeterminant) in.getValue(2));
                        }
                    }
                }
                long latency = System.nanoTime() - (long)in.getValue(3);
                this.latency.addValue(latency / 1E6);
                if ((System.nanoTime() - computationLatency) / 1E9 > 0.5) {
                    No_Exactly_Once_latency_map.add(latency / 1E6);
                    computationLatency = System.nanoTime();
                }
                count ++;
            }
        }
        if (failureFlagBid.contains(in.getBID())) {
            MeasureTools.ReExecute_time_finish(System.nanoTime());
        }
    }
    private void measure_end() {
        LOG.info("System running time is " + (System.nanoTime() - startTime) / 1E9);
        LOG.info("Transactions abort : " + abortTransaction);
        MeasureTools.setAvgThroughput(thisTaskId,exe * 1E6 / (System.nanoTime() - startTime));
        for (double a:throughput_map){
            throughput.addValue(a);//k events/s
        }
        MeasureTools.setThroughputMap(thisTaskId, throughput_map);
        MeasureTools.setLatencyMap(thisTaskId, No_Exactly_Once_latency_map);
        MeasureTools.setLatency(thisTaskId, latency);
        MeasureTools.setCheckpointTimes(CheckpointTimes);
        MeasureTools.setSystemRuntime((long) ((System.nanoTime() - startTime) / 1E9));
        MeasureTools.setResultsMap(thisTaskId,results);
    }

    public long getCount() {
        return count;
    }

    public void startThroughputMeasure(){
        SinkTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long current_count = getCount();
                double throughput = (current_count - p_count) / 1000.0;
                p_count = current_count;
                throughput_map.add(throughput);
            }
        },  1000, 1000);//ms
    }
    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
