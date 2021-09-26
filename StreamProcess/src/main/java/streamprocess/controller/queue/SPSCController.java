package streamprocess.controller.queue;

import streamprocess.execution.ExecutionNode;

import java.util.HashMap;
import java.util.Queue;

/**
 * PC is owed by streamController, which is owned by each executor
 * SPSC implementation -- Queue is unique to each producer and consumer
 */
public class SPSCController extends QueueController{
    SPSCController(HashMap<Integer, ExecutionNode> downExecutor_list) {
        super(downExecutor_list);
    }

    @Override
    public Queue get_queue(int executor) {
        return null;
    }

    @Override
    public void allocate_queue(boolean linked, int desired_elements_epoch_per_core) {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
