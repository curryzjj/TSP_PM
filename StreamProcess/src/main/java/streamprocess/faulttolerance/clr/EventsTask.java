package streamprocess.faulttolerance.clr;

import java.io.Serializable;

/**
 * One task access the shared state, tasks may influence each other
 * So, we combine the tasks in one EventTask
 * Task need to support Undo operation, and need to support Fine-grained recovery
 * So, we divide the EventTask into multiple TransactionTasks, and then TransactionTask are divide into OperationsTask
 * Spout->EventTask->Bolts->TransactionsTasks->TxnManager->OperationsTask->DB
 */
public class EventsTask implements Serializable {
    private static final long serialVersionUID = -8306359454629589737L;
}
