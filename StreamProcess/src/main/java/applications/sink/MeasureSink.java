package applications.sink;

import System.sink.helper.stable_sink_helper;
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

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

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
    /** <bid,timestamp> */
    protected List<Tuple2<Long, Long>> perCommitTuple=new ArrayList<>();
    protected long currentCommitBid=0;
    protected int abortTransaction=0;

    public MeasureSink() {
        super(new HashMap<>());
        this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put("tn", 1.0);
        status=new Status();
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
    public void execute(Tuple in) throws InterruptedException {
        if(in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                this.collector.ack(in,in.getMarker());
                if(in.getMarker().getValue()=="recovery"){
                    this.registerRecovery();
                }else if(in.getMarker().getValue()=="snapshot"){
                    CommitTuple(in.getBID());
                }else if(in.getMarker().getValue()=="finish"){
                    measure_end();
                }else if(enable_wal){
                    CommitTuple(in.getBID());
                }
            }
        }else{
            boolean finish= (boolean) in.getValue(0);
            if(!finish){
                LOG.info("The tuple ("+in.getBID()+ ") is abort");
                abortTransaction++;
            }else{
                perCommitTuple.add(new Tuple2(in.getBID(),in.getValue(1)));
            }
        }
    }

    private void measure_end() {

    }

    private void CommitTuple(long bid) {
        if(enable_latency_measurement){
            long totalLatency=0L;
            long size=0;
            Iterator<Tuple2<Long, Long>> events=perCommitTuple.iterator();
            while(events.hasNext()){
                Tuple2<Long,Long> event=events.next();
                if(event._1().longValue()<bid){
                    final long end = System.nanoTime();
                    final long process_latency = end - event._2;//ns
                    totalLatency=totalLatency+process_latency;
                    size++;
                }
                events.remove();
            }
            latency_map.add(totalLatency/size);
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
    protected Logger getLogger() {
        return LOG;
    }
}
