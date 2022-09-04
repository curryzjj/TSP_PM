package streamprocess.faulttolerance.checkpoint;

import System.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.Serializable;
import java.util.HashMap;

import static UserApplications.CONTROL.enable_debug;

public class Status implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Status.class);
    private static final long serialVersionUID = -6190161913336090702L;
    private transient HashMap<Integer,Boolean> source_ready;
    private transient HashMap<Integer,Boolean> consumer_ack;
    public Status(){
        OsUtils.configLOG(LOG);
    }
    public void source_status_ini(ExecutionNode executor) {
        if (source_ready == null) {
            source_ready = new HashMap<>();
            for (TopologyComponent op : executor.getParents_keySet()) {
                for (ExecutionNode src : op.getExecutorList()) {
                    source_ready.put(src.getExecutorID(), false);
                }
            }
        } else {
            source_ready.replaceAll((i, v) -> false);
        }
    }

    public void dst_status_init(ExecutionNode executor) {
        if (consumer_ack == null) {
            consumer_ack = new HashMap<>();
            for (TopologyComponent op : executor.getChildren_keySet()) {
                for (ExecutionNode dst : op.getExecutorList()) {
                    consumer_ack.put(dst.getExecutorID(), false);
                }
            }
        } else {
            consumer_ack.replaceAll((i, v) -> false);
        }
    }
    public synchronized boolean allMarkerArrived(int callee, ExecutionNode executor){
        source_ready.put(callee, true);
        if(all_src_arrived()){
            source_status_ini(executor);
            return true;
        }else {
            return false;
        }
    }
    public synchronized boolean isMarkerArrived(int callee){
        return source_ready.get(callee);
    }
    /**
     * have received all marker from source
     * @return
     */
    boolean all_src_arrived() {
        return !(source_ready.containsValue(false));
    }

    /**
     * have received all ack from consumers.
     *
     * @return
     */
    boolean all_dst_ack() {
        return !(consumer_ack.containsValue(false));
    }
    public synchronized void callback_bolt(int callee, Tuple message, ExecutionNode executor) {
        consumer_ack.put(callee, true);
        if (all_dst_ack()) {
            LOG.info(executor.getOP_full() + " received ack of message" + message.getFailureFlag().msgId + " from all consumers.");
            dst_status_init(executor);//reset state.
        }
    }

    public synchronized void callback_spout(int callee, Tuple message, ExecutionNode executor) {
        consumer_ack.put(callee, true);
        if (all_dst_ack()) {
            if (enable_debug)
                LOG.info(executor.getOP_full() + " received ack of message "+ message.getFailureFlag().msgId + " from all consumers.");
            dst_status_init(executor);
        }
    }
}
