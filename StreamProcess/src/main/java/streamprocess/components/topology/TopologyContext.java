package streamprocess.components.topology;
/**
 * A TopologyContext is created for each executor.
 * It is given to bolts and spouts in their "Loading" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the StreamProcess.topology, such as Task ids, inputs and outputs, etc.
 * <profiling/>
 */
public class TopologyContext {
    public TopologyComponent getComponent(int taskId) {
        return null;
    }

    public int getThisTaskIndex() {
        return 0;
    }
}
