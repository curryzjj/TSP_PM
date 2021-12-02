package applications.spout.transactional;

import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.TransactionalSpoutFT;
import streamprocess.execution.ExecutionGraph;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.IOException;

import static UserApplications.CONTROL.NUM_EVENTS;

public class MemFileTransactionalSpoutFT extends TransactionalSpoutFT {
    private static final Logger LOG= LoggerFactory.getLogger(MemFileTransactionalSpoutFT.class);
    public MemFileTransactionalSpoutFT(){
        super(LOG);
        this.scalable=false;
        status=new Status();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("MemFileTransactionalSpout initialize is being called");
        cnt = 0;
        counter = 0;
        this.graph=graph;
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        load_input();
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
    public void nextTuple(int batch) throws InterruptedException {
        if(exe!=1){
            if(exe==NUM_EVENTS){
                clock.start();
            }
            forward_marker(this.taskId, bid, null,"finish");
            collector.emit(array_array[counter],bid);
            bid++;
            counter++;
            if(counter==array_array.length){
                counter=0;
            }
            exe--;
        }else {
            try {
                clock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            collector.emit(array_array[counter],bid);
            if (taskId == graph.getSpout().getExecutorID()) {
                LOG.info("Thread:" + taskId + " is going to stop all threads sequentially");
                context.stop_running();
            }
        }
    }

    @Override
    protected void loadReplay() {

    }

    @Override
    public void recoveryInput(long offset) {

    }

}
