package streamprocess.controller.queue;

import streamprocess.execution.ExecutionNode;

import java.util.HashMap;
import java.util.Queue;
/**
 * PC is owed by streamController, which is owned by each executor
 * MPSC implementation -- Queue is shared among multiple executors of the same producer
 */
public class MPSCController extends QueueController{
    MPSCController(HashMap<Integer, ExecutionNode> downExecutor_list) {
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
