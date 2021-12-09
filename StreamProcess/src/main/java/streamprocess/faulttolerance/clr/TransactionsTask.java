package streamprocess.faulttolerance.clr;

import java.io.Serializable;

/**
 * One task access the shared state, tasks may influence each other
 * So, we combine the tasks in one EventTask
 * Task need to support Undo operation, and need to support Fine-grained recovery
 * So, we divide the EventTask into multiple TransactionTasks
 * Spout->EventTask->Bolts->TransactionTasks->DB
 */
public class TransactionsTask implements Serializable {
    private static final long serialVersionUID = 273540275071583336L;
}
