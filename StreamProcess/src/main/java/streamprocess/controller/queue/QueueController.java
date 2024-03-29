package streamprocess.controller.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionNode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Queue;

public abstract class QueueController implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(QueueController.class);
    private static final long serialVersionUID = -6212701358133777285L;
    final HashMap<Integer, ExecutionNode> downExecutor_list;
    QueueController(HashMap<Integer,ExecutionNode> downExecutor_list){
        this.downExecutor_list=downExecutor_list;
    }
    public abstract Queue get_queue(int executor);
    public abstract void allocate_queue(boolean linked,int desired_elements_epoch_per_core);
    public abstract boolean isEmpty();

    public abstract void cleanAll();
    public abstract void clean(int executeId);
}
