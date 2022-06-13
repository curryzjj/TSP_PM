package applications.sink;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.measure.MeasureTools;
import System.util.Configuration;
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

import java.io.*;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public class MeasureSink extends BaseSink {
    private static final long serialVersionUID = 6249684803036342603L;
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private final DescriptiveStatistics latency = new DescriptiveStatistics();
    private final DescriptiveStatistics waitTime = new DescriptiveStatistics();
    private final DescriptiveStatistics throughput = new DescriptiveStatistics();
    private DescriptiveStatistics twoPC_commit_time = new DescriptiveStatistics();
    private long commitStartTime;
    protected long startTime;
    private FileSystem localFS;
    protected static boolean profile = false;
    private int exe;
    protected final List<Double> latency_map = new ArrayList<>();
    protected static final List<Double> throughput_map = new ArrayList<>();
    public int batch_number_per_wm;
    /** <bid,timestamp> */

    //2PC
    protected HashMap<Long, Tuple2<Long,Long>> perCommitTuple = new HashMap<>();
    //no_Exactly_once
    protected final List<Double> No_Exactly_Once_latency_map =new ArrayList<>();
    protected int abortTransaction=0;
    protected static long count;
    protected static long  p_count;
    public MeasureSink() {
        super(new HashMap<>());
        this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put("tn", 1.0);
        status=new Status();
        this.localFS=new LocalFileSystem();
        if(enable_measure){
            count=0;
            p_count=0;
        }
        this.startThroughputMeasure();
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

    @Override
    public void execute(Tuple in) throws InterruptedException, IOException {
        if (count == 0){
            startTime = System.nanoTime();
        }
        if(in.isMarker()){
            if (enable_recovery_dependency) {
                addRecoveryDependency(in.getBID());
                this.recoveryDependency.get(in.getBID()).addDependency(in.getMarker().getEpochInfo());
            }
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                this.currentMarkerId = in.getBID();
                if (enable_determinants_log) {
                    for (Integer id:this.causalService.keySet()){
                        causalService.get(id).setCurrentMarkerId(currentMarkerId);
                    }
                }
                if(Objects.equals(in.getMarker().getValue(), "recovery")){
                    MeasureTools.finishRecovery(System.nanoTime());
                } else if(Objects.equals(in.getMarker().getValue(), "snapshot")) {
                    this.FTM.sinkRegister(in.getBID());
                } else if(Objects.equals(in.getMarker().getValue(), "finish")){
                    if(Exactly_Once){
                        twoPC_CommitTuple(in.getBID());
                    }
                    timer.cancel();
                    measure_end();
                    context.stop_running();
                } else {
                    BUFFER_EXECUTE();
                }
            }
        }else{
           execute_ts_normal(in);
        }
    }


    @Override
    public void execute(JumboTuple in) throws DatabaseException, BrokenBarrierException, InterruptedException {
        for(int i=0;i<in.length;i++){
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
            double totalLatency=0;
            double totalWaitTime=0;
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
                    if(size==0){
                        latency_map.add(totalLatency/5000);
                        totalLatency=0;
                        size=5000;
                    }
                }
            }
            if (size !=0) {
                latency_map.add(totalLatency/(5000-size));
            }
            waitTime.addValue(totalWaitTime/commitSize);
            twoPC_commit_time.addValue((System.nanoTime()-commitStartTime)/1E6);
        }
    }

    @Override
    protected void BUFFER_EXECUTE() throws IOException, InterruptedException {
        for (Queue<Tuple> tuples : bufferedTuples.values()) {
            if (tuples.size() !=0) {
                boolean isMarker = false;
                while (!isMarker) {
                    Tuple tuple = tuples.poll();
                    if (tuple != null) {
                        execute(tuple);
                        if (tuple.isMarker()) {
                            isMarker =true;
                        }
                    } else {
                        isMarker = true;
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
        boolean finish = (boolean) in.getValue(0);
        if (!finish) {
            LOG.info("The tuple ("+in.getBID()+ ") is abort");
            if (enable_determinants_log) {
                if (in.getValue(1) != null) {
                    InsideDeterminant insideDeterminant = (InsideDeterminant) in.getValue(1);
                    this.causalService.get(insideDeterminant.partitionId).addAbortEvent(insideDeterminant.input);
                }
            }
            abortTransaction++;
        } else {
            if (Exactly_Once) {
                perCommitTuple.put(in.getBID(),new Tuple2<>((long)in.getValue(2),System.nanoTime()));
            } else {
                if (enable_determinants_log) {
                    if (in.getValue(1) != null) {
                        if (in.getValue(1) instanceof InsideDeterminant) {
                            InsideDeterminant insideDeterminant = (InsideDeterminant) in.getValue(1);
                            this.causalService.get(insideDeterminant.partitionId).addInsideDeterminant(insideDeterminant);
                        } else {
                            for (int targetPartition:((OutsideDeterminant) in.getValue(1)).targetPartitionIds) {
                                this.causalService.get(targetPartition).addOutsideDeterminant((OutsideDeterminant) in.getValue(1));
                            }
                        }
                    }
                }
                long latency = System.nanoTime() - (long)in.getValue(2);
                No_Exactly_Once_latency_map.add(latency/1E6);
                count++;
            }
        }
    }
    private void addLatency() {
        if(No_Exactly_Once_latency_map.size()!=0){
            double totalLatency=0;
            for (double latency: No_Exactly_Once_latency_map){
                totalLatency=totalLatency+latency;
            }
            latency_map.add(totalLatency/ No_Exactly_Once_latency_map.size());
            No_Exactly_Once_latency_map.clear();
        }
    }
    private void measure_end() {
        MeasureTools.setAvgThroughput(thisTaskId,count*1E6/(System.nanoTime()-startTime));
        for (double a:latency_map){
            latency.addValue(a);
        }
        for (double a:throughput_map){
            throughput.addValue(a);//k events/s
        }
        MeasureTools.setThroughputMap(thisTaskId,throughput_map);
        MeasureTools.setLatencyMap(thisTaskId,latency_map);
        MeasureTools.setAvgLatency(thisTaskId,latency.getMean());
        MeasureTools.setTailLatency(thisTaskId,latency.getPercentile(0.9));
        MeasureTools.setAvgWaitTime(thisTaskId,waitTime.getMean());
        MeasureTools.setAvgCommitTime(thisTaskId,twoPC_commit_time.getMean());
    }

    public long getCount() {
        return count;
    }

    public void startThroughputMeasure(){
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long current_count=getCount();
                double throughput=(current_count-p_count)/1000.0;
                p_count=current_count;
                throughput_map.add(throughput);
            }
        },  1000, 1000);//ms
    }
    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
