package streamprocess.faulttolerance.clr;
/**
 * One task access the shared state, tasks may influence each other
 * So, we combine the tasks in one EventTask
 * Task need to support Undo operation, and need to support Fine-grained recovery
 * So, we divide the EventTask into multiple TransactionTasks, and then TransactionTask are divide into OperationsTask
 * Spout->EventTask->Bolts->TransactionsTasks->TxnManager->OperationsTask->DB
 */

import java.io.Serializable;
/**
 * OperationsTask record the operations to partitioned states
 */
public class OperationsTask implements Serializable {
    private static final long serialVersionUID = 4822059630736962439L;

}
