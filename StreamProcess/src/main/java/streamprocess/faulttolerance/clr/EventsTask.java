package streamprocess.faulttolerance.clr;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static UserApplications.CONTROL.partition_num;
/**
 * One task access the shared state, tasks may influence each other
 * So, we combine the tasks in one EventTask
 * Task need to support Undo operation, and need to support Fine-grained recovery
 * So, we divide the EventTask into multiple TransactionTasks, and then TransactionTask are divide into OperationsTask
 * Spout->EventTask->Bolts->TransactionsTasks->TxnManager->OperationsTask->DB
 */
public class EventsTask implements Serializable {
    private static final long serialVersionUID = -8306359454629589737L;
    private long TaskId;
    private final ConcurrentHashMap<Integer,ConcurrentLinkedQueue<ComputationTask>> taskQueues;
    private final ConcurrentHashMap<Integer,ConcurrentLinkedQueue<Long>> bidQueues;
    private final ConcurrentLinkedQueue<ComputationLogic> computationLogicsQueues;//concurrent
    public EventsTask(long taskId){
        this.TaskId=taskId;
        taskQueues=new ConcurrentHashMap<>();
        bidQueues = new ConcurrentHashMap<>();
        computationLogicsQueues =new ConcurrentLinkedQueue<ComputationLogic>();
        for (int i=0;i<partition_num;i++){
            taskQueues.put(i,new ConcurrentLinkedQueue<>());
            bidQueues.put(i,new ConcurrentLinkedQueue<>());
        }
    }
    public void addComputationTask(List<ComputationTask> tasks){
        for (ComputationTask task:tasks){
            ConcurrentLinkedQueue<ComputationTask> taskQueue=taskQueues.get(task.getPartition_id());
            taskQueue.add(task);
        }
    }
    public void addComputationLogic(List<ComputationLogic> logics){
        for (ComputationLogic logic:logics){
            for (int i=0;i<partition_num;i++){
                if (!logic.haveTaskOnPartition(i)){
                    this.bidQueues.get(i).add(logic.getBid());
                }
            }
        }
    }

    public long getTaskId() {
        return TaskId;
    }
    public ConcurrentLinkedQueue<ComputationTask> getComputationTasksByPartitionId(int partitionId){
        return taskQueues.get(partitionId);
    }

    public ConcurrentLinkedQueue<ComputationLogic> getComputationLogicsQueues() {
        return computationLogicsQueues;
    }

    public ConcurrentLinkedQueue<Long> getBidQueuesByPartitionId(int partitionId) {
        return bidQueues.get(partitionId);
    }
}