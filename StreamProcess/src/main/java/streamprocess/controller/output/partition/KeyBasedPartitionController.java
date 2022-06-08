package streamprocess.controller.output.partition;

import System.util.Configuration;
import System.util.DataTypes.StreamValues;
import applications.events.TxnEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.PartitionController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.Meta;

import java.util.HashMap;
import java.util.LinkedList;

public class KeyBasedPartitionController extends PartitionController {
    private static final long serialVersionUID = -3413701485091517468L;
    private static Logger LOG = LoggerFactory.getLogger(KeyBasedPartitionController.class);
    /**
     * @param operator
     * @param childOP
     * @param downExecutor_list
     * @param batch_size
     * @param executionNode     if this is null, it is a shared PC, otherwise, it is unique to each executor
     * @param common
     * @param log
     * @param profile
     * @param conf
     */
    public KeyBasedPartitionController(TopologyComponent operator, TopologyComponent childOP, HashMap<Integer, ExecutionNode> downExecutor_list, int batch_size, ExecutionNode executionNode, boolean common, Logger log, boolean profile, Configuration conf) {
        super(operator, childOP, downExecutor_list, batch_size, executionNode, common, log, profile, conf);
    }
    public KeyBasedPartitionController(TopologyComponent operator, TopologyComponent childOP, HashMap<Integer, ExecutionNode> downExecutor_list, int batch_size, ExecutionNode executionNode, boolean common, boolean profile, Configuration conf) {
        super(operator, childOP, downExecutor_list, batch_size, executionNode, common, LOG, profile, conf);
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, Object output) throws InterruptedException {
        return 0;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        int target = this.targetTasks[((TxnEvent) output[0]).getPid()];
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }

    @Override
    public int force_emit_ID(Meta meta, String streamId, int targetId, long bid, Object... output) throws InterruptedException {
        force_offer(meta.src_id, targetId, streamId, bid, output);
        return targetId;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException {
        return 0;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        return 0;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_inorder_push(Meta meta, String streamId, long bid, LinkedList<Long> gap) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        return 0;
    }

    @Override
    public int emit_nowait(Meta meta, String streamId, Object... output) {
        return 0;
    }

    @Override
    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        return 0;
    }

    @Override
    public int emit_nowait(Meta meta, String streamId, char[] output) throws InterruptedException {
        return 0;
    }
}
