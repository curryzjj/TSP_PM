package applications.spout;

import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.execution.ExecutionGraph;

import java.io.BufferedWriter;

public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
    protected int element = 0;

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
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        load_input();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple() throws InterruptedException {
        //wait for the OutputCollector
        collector.emit(array_array[counter]);
        counter++;
        if(counter==array_array.length){
            counter=0;
        }
    }

    @Override
    public void nextTuple_nonblocking() throws InterruptedException {
        //wait for the OutputCollector
        collector.emit_nowait(array_array[counter]);
        counter++;
        if(counter==array_array.length){
            counter=0;
        }
    }
}
