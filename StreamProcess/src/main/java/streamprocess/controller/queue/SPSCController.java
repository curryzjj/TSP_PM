package streamprocess.controller.queue;

import System.util.OsUtils;
import org.jctools.queues.SpscArrayQueue;
import org.jctools.queues.SpscLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionNode;

import java.util.HashMap;
import java.util.Queue;

/**
 * PC is owed by streamController, which is owned by each executor
 * SPSC implementation -- Queue is unique to each producer and consumer
 */
public class SPSCController extends QueueController{
    private static final Logger LOG= LoggerFactory.getLogger(SPSCController.class);
    private final HashMap<Integer,Queue> outputQueue=new HashMap<>();////<Downstream executor ID, corresponding output queue>
    public SPSCController(HashMap<Integer, ExecutionNode> downExecutor_list) {
        super(downExecutor_list);
    }

    @Override
    public Queue get_queue(int executor) {
        return outputQueue.get(executor);
    }

    @Override
    public void allocate_queue(boolean linked, int desired_elements_epoch_per_core) {
        for(int executor:downExecutor_list.keySet()){
            Queue queue=outputQueue.get(executor);
            if(queue!=null){
                LOG.info("relax_reset the old queue");
                queue.clear();
                System.gc();
            }
            if(OsUtils.isWindows()||OsUtils.isMac()){
                outputQueue.put(executor,new SpscArrayQueue(1024));
            }else{
                if(linked){
                    outputQueue.put(executor,new SpscLinkedQueue());
                }else{
                    outputQueue.put(executor,new SpscArrayQueue(desired_elements_epoch_per_core/2));
                }
            }
        }
    }

    @Override
    public boolean isEmpty() {
        for (int executor : downExecutor_list.keySet()) {
            Queue queue = outputQueue.get(executor);
            if (!queue.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
