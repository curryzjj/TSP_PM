package applications.sink;

import System.FileSystem.FileSystem;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.measure.MeasureTools;
import System.sink.helper.stable_sink_helper;
import System.util.Configuration;
import System.util.OsUtils;
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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static System.Constants.Mac_Measure_Path;
import static System.Constants.Node22_Measure_Path;
import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private static final DescriptiveStatistics latency = new DescriptiveStatistics();
    private static final DescriptiveStatistics waitTime=new DescriptiveStatistics();
    private static final DescriptiveStatistics throughput=new DescriptiveStatistics();
    private Path result_Path;
    private FileSystem localFS;
    private File resultFile;
    private static final long serialVersionUID = 6249684803036342603L;
    protected static boolean profile = false;
    protected stable_sink_helper helper;
    private int exe;
    protected final ArrayDeque<Long> latency_map = new ArrayDeque();
    protected static final ArrayDeque<Long> throughput_map=new ArrayDeque<>();
    public int batch_number_per_wm;
    /** <bid,timestamp> */
    //protected List<Tuple2<Long, Long>> perCommitTuple=new ArrayList<>();

    //2PC
    protected HashMap<Long, Tuple2<Long,Long>> perCommitTuple=new HashMap<>();
    //no_commit
    protected final ArrayDeque<Long> no_commit_latency_map=new ArrayDeque<>();
    protected long currentCommitBid=0;
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
        if(in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                if(Objects.equals(in.getMarker().getValue(), "recovery")){
                    MeasureTools.finishRecovery(System.nanoTime());
                }else if(Objects.equals(in.getMarker().getValue(), "finish")){
                    timer.cancel();
                    measure_end(in.getBID());
                    context.stop_running();
                }else {
                    if(Exactly_Once){
                        CommitTuple(in.getBID());
                    }else{
                        addLatency();
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
                    no_commit_latency_map.add((long) (latency/1E6));
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
            MeasureTools.twoPC_commit_begin(System.nanoTime());
            long totalLatency=0L;
            long totalWaitTime=0L;
            long size=0;
            Iterator<Map.Entry<Long, Tuple2<Long, Long>>> events = perCommitTuple.entrySet().iterator();
            while(events.hasNext()){
                Map.Entry<Long, Tuple2<Long, Long>> event = events.next();
                if(event.getKey()<bid){
                    final long end = System.nanoTime();
                    final long process_latency = (long) ((end - event.getValue()._1)/1E6);//ms
                    final long wait_latency=(long) ((end - event.getValue()._2)/1E6);//ms
                    totalLatency=totalLatency+process_latency;
                    totalWaitTime=totalWaitTime+wait_latency;
                    size++;
                    count++;
                    events.remove();
                }
            }
            latency_map.add(totalLatency/size);
            waitTime.addValue(totalWaitTime/size);
            MeasureTools.twoPC_commit_finish(System.nanoTime());
        }
    }
    private void addLatency() {
        if(no_commit_latency_map.size()!=0){
            long totalLatency=0;
            for (long latency:no_commit_latency_map){
                totalLatency=totalLatency+latency;
            }
            latency_map.add(totalLatency/no_commit_latency_map.size());
            no_commit_latency_map.clear();
        }
    }
    private void measure_end(long bid) throws IOException {
        if(perCommitTuple.size()!=0){
            CommitTuple(bid);
        }
        if(OsUtils.isMac()){
            this.result_Path=new Path(Mac_Measure_Path,getConfigPrefix()+"_"+"Latency"+"_"+config.getInt("FTOptions")+"_"+config.getInt("failureModel")+"_"+config.getInt("failureTime")+"_"+config.getInt("executor.threads"));
        }else{
            this.result_Path=new Path(Node22_Measure_Path,getConfigPrefix()+"_"+"Latency"+"_"+config.getInt("FTOptions")+"_"+config.getInt("failureModel")+"_"+config.getInt("failureTime")+"_"+config.getInt("executor.threads"));
        }
        Path parent=result_Path.getParent();
        if (parent != null && !localFS.mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        resultFile=localFS.pathToFile(result_Path);
        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(resultFile);
        DataOutputStream dataOutputStream=new DataOutputStream(localDataOutputStream);
        StringBuilder sb = new StringBuilder();
        for (Long a:latency_map){
            latency.addValue(a);
            dataOutputStream.writeUTF(String.valueOf(a));
            dataOutputStream.write(new byte[]{13,10});
        }
        sb.append("=======Latency Details=======");
        sb.append("\n" + latency.toString() + "\n");
        sb.append("===99th===" + "\n");
        sb.append(latency.getPercentile(99) + "\n");
        LOG.info(sb.toString());
        if(OsUtils.isMac()){
            this.result_Path=new Path(Mac_Measure_Path,getConfigPrefix()+"_"+"Throughput"+"_"+config.getInt("FTOptions")+"_"+config.getInt("failureModel")+"_"+config.getInt("failureTime")+"_"+config.getInt("executor.threads"));
        }else{
            this.result_Path=new Path(Node22_Measure_Path,getConfigPrefix()+"_"+"Throughput"+"_"+config.getInt("FTOptions")+"_"+config.getInt("failureModel")+"_"+config.getInt("failureTime")+"_"+config.getInt("executor.threads"));
        }
        resultFile=localFS.pathToFile(result_Path);
        localDataOutputStream=new LocalDataOutputStream(resultFile);
        dataOutputStream=new DataOutputStream(localDataOutputStream);
        sb = new StringBuilder();
        for (Long a:throughput_map){
            throughput.addValue(a);//k events/s
            dataOutputStream.writeUTF(String.valueOf(a));
            dataOutputStream.write(new byte[]{13,10});
        }
        sb.append("=======WaitTime Details=======");
        sb.append("\n" + waitTime.toString() + "\n");
        dataOutputStream.close();
        localDataOutputStream.close();
        LOG.info(sb.toString());
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
                long throughput=(current_count-p_count)/1000;
                p_count=current_count;
                throughput_map.add(throughput);
            }
        },  1000, 1000);//ms
    }
}
