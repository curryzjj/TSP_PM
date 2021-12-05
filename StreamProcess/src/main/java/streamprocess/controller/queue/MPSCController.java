package streamprocess.controller.queue;

import System.util.OsUtils;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscLinkedQueue8;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
/**
 * PC is owed by streamController, which is owned by each executor
 * MPSC implementation -- Queue is shared among multiple executors of the same producer
 */
public class MPSCController extends QueueController{
    private static final Logger LOG= LoggerFactory.getLogger(MPSCController.class);
    private Map<Integer,Queue> outputQueue=new HashMap<>();//<Downstream executor Id, corresponding output queue>
    public MPSCController(HashMap<Integer, ExecutionNode> downExecutor_list) {
        super(downExecutor_list);
        //Queue temp1=new MpscArrayQueue(1024);//Don't why need this
    }

    @Override
    public Queue get_queue(int executor) {
        return outputQueue.get(executor);
    }
    /**
     * Allocate memory for queue structure here.
     *
     * @param linked
     * @param desired_elements_epoch_per_core
     */
    @Override
    public void allocate_queue(boolean linked, int desired_elements_epoch_per_core) {
        Queue temp1=new MpscArrayQueue(1024);//Don't why need this
        for(int executor:downExecutor_list.keySet()){
            if(OsUtils.isWindows()||OsUtils.isMac()){
                outputQueue.put(executor,new MpscArrayQueue(desired_elements_epoch_per_core));//TODO:fixed the error
            }else{
                if(linked){
                    outputQueue.put(executor,new MpscLinkedQueue8<>());
                }else{
                    outputQueue.put(executor,new MpscArrayQueue(desired_elements_epoch_per_core));
                }
            }
        }
    }

    @Override
    public boolean isEmpty() {
        for(int executor:downExecutor_list.keySet()){
            Queue queue=outputQueue.get(executor);
            if(!queue.isEmpty()){
                return false;
            }
        }
        return true;
    }

    @Override
    public void clean() {
        for (Queue q:this.outputQueue.values()){
            q.clear();
        }
    }
}
