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
    private final DescriptiveStatistics waitTime=new DescriptiveStatistics();
    private final DescriptiveStatistics throughput=new DescriptiveStatistics();
    private DescriptiveStatistics twoPC_commit_time=new DescriptiveStatistics();
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
    protected HashMap<Long, Tuple2<Long,Long>> perCommitTuple=new HashMap<>();
    //no_commit
    protected final List<Double> no_commit_latency_map=new ArrayList<>();
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
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                if(Objects.equals(in.getMarker().getValue(), "recovery")){
                    MeasureTools.finishRecovery(System.nanoTime());
                }else {
                    if(Exactly_Once){
                        CommitTuple(in.getBID());
                    }else{
                        addLatency();
                    }
                    if(Objects.equals(in.getMarker().getValue(), "finish")){
                        timer.cancel();
                        measure_end();
                        context.stop_running();
                    }
                }
            }
        }else{
            boolean finish= (boolean) in.getValue(0);
            if(!finish){
                LOG.info("The tuple ("+in.getBID()+ ") is abort");
                abortTransaction++;
            }else{
                if(Exactly_Once){
                    perCommitTuple.put(in.getBID(),new Tuple2<>((long)in.getValue(1),System.nanoTime()));
                }else{
                    long latency=System.nanoTime()-(long)in.getValue(1);
                    no_commit_latency_map.add(latency/1E6);
                    count++;
                }
            }
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

    private void CommitTuple(long bid) {
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
    private void addLatency() {
        if(no_commit_latency_map.size()!=0){
            double totalLatency=0;
            for (double latency:no_commit_latency_map){
                totalLatency=totalLatency+latency;
            }
            latency_map.add(totalLatency/no_commit_latency_map.size());
            no_commit_latency_map.clear();
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

    @Override
    protected Logger getLogger() {
        return LOG;
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

}
