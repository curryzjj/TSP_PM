package streamprocess.faulttolerance.checkpoint;

import System.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.Serializable;
import java.util.HashMap;

import static UserApplications.CONTROL.enable_debug;

public class Status<E extends Serializable> implements Serializable {
    private static final Logger LOG= LoggerFactory.getLogger(Status.class);
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
            for (Integer integer : source_ready.keySet()) {
                source_ready.put(integer, false);
            }
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
            for (Integer integer : consumer_ack.keySet()) {
                consumer_ack.put(integer, false);
            }
        }
    }
    /**
     * have received all ack from consumers.
     *
     * @return
     */
    boolean all_dst_ack() {
        return !(consumer_ack.containsValue(false));
    }
    public synchronized void callback_bolt(int callee, Marker marker, ExecutionNode executor) {
        consumer_ack.put(callee, true);
        if (all_dst_ack()) {
            LOG.info(executor.getOP_full() + " received ack of marker"+marker.msgId+" from all consumers.");
            //	writer.save_state_MMIO_synchronize(executor); // enable if fault-tolerance enabled.
            dst_status_init(executor);//reset state.
            executor.clean_status(marker);
        }
    }

    public synchronized void callback_spout(int callee, Marker marker, ExecutionNode executor) {
        consumer_ack.put(callee, true);
//        executor.earlier_clean_state(marker);
        if (all_dst_ack()) {
            if (enable_debug)
                LOG.info(executor.getOP_full() + " received ack of marker "+marker.msgId+" from all consumers.");
            dst_status_init(executor);
            executor.clean_status(marker);
        }
    }
}
