package streamprocess.controller.input.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.controller.input.InputStreamController;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.optimization.model.STAT;

import java.util.LinkedList;
import java.util.Queue;

public class SequentialScheduler extends InputStreamController {
    private static final Logger LOG= LoggerFactory.getLogger(SequentialScheduler.class);
    private final LinkedList<Queue> LQ=new LinkedList<>();
    private int size;
    private int current=0;

    @Override
    public void initialize() {
        super.initialize();
        for(String streamId:keySet){
            LQ.addAll(getRQ().get(streamId).values());
        }
        size=LQ.size();
        current=0;
        if(size==0){
            LOG.info("MyQueue initialize wrong");
            System.exit(-1);
        }
    }

    @Override
    public JumboTuple fetchResults_inorder() {
        if(current==size){
            current=0;
        }
        return fetchFromqueue_inorder(LQ.get(current++));
    }

    @Override
    public Object fetchResults() {
        if (current == size) {
            current = 0;
        }
        return fetchFromqueue(LQ.get(current++));
    }

    @Override
    public Tuple fetchResults_single() {
        if (current == size) {
            current = 0;
        }
        return fetchFromqueue_single(LQ.get(current++));
    }

    @Override
    public JumboTuple fetchResults(int batch) {
        if (current == size) {
            current = 0;
        }
        return fetchFromqueue(LQ.get(current++), batch);
    }
}
