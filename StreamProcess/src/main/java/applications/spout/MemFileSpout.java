package applications.spout;

import System.util.Configuration;
import engine.shapshot.SnapshotResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.execution.ExecutionGraph;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.util.List;

public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
    protected int element = 0;

    long meauseStartTime;
    long meauseLatencyStartTime;
    long meauseFinishTime;

    private transient BufferedWriter writer;
    public MemFileSpout(){
        super(LOG);
        this.scalable=false;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("Spout initialize is being called");
        cnt = 0;
        counter = 0;
        this.graph=graph;
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        load_input();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple(int batch) throws InterruptedException {
        if(exe!=1){
            collector.emit(array_array[counter]);
            counter++;
            if(counter==array_array.length){
                counter=0;
            }
            exe--;
        }else {
            collector.emit(array_array[counter]);
            if (taskId == graph.getSpout().getExecutorID()) {
                LOG.info("Thread:" + taskId + " is going to stop all threads sequentially");
                context.stop_running();
            }
        }

    }


    @Override
    public void nextTuple_nonblocking(int batch) throws InterruptedException {
        collector.emit_nowait(array_array[counter]);
        counter++;
        if(counter==array_array.length){
            counter=0;
        }
    }

    @Override
    public void recoveryInput(long offset, List<Integer> recoveryExecutorIds) throws FileNotFoundException, InterruptedException {

    }
}
