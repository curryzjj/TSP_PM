package streamprocess.controller.output.partition;

import System.util.Configuration;
import System.util.DataTypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.PartitionController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.Meta;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

public class ShufflePartitionController extends PartitionController {
    private static Logger LOG = LoggerFactory.getLogger(ShufflePartitionController.class);

    public ShufflePartitionController(TopologyComponent operator, TopologyComponent childOP, HashMap<Integer
            , ExecutionNode> downExecutor_list, int batch, ExecutionNode executor, boolean common, Logger LOG, boolean profile, Configuration conf) {
        super(operator, childOP, downExecutor_list, batch, executor, common, LOG, profile, conf);
        initilize();
    }

    public ShufflePartitionController(TopologyComponent operator, TopologyComponent childOP, HashMap<Integer
            , ExecutionNode> downExecutor_list, int batch, ExecutionNode executor, boolean common, boolean profile, Configuration conf) {
        super(operator, childOP, downExecutor_list, batch, executor, common, LOG, profile, conf);
        initilize();
    }

    public void initilize() {
        Set<Integer> setID = super.getDownExecutor_list().keySet();
        targetTasks = setID.toArray(new Integer[setID.size()]);
        updateExtendedTargetId();
    }

    /**
     * no shuffle for better cache locality?
     */
    public void updateExtendedTargetId() {


        double min_ratio = Double.MAX_VALUE;

        for (int e : partition_ratio.keySet()) {
            double ratio = partition_ratio.get(e);
            if (ratio < min_ratio) {
                min_ratio = ratio;
            }
        }
        int range = (int) Math.ceil(this.batch / min_ratio);

        for (int e : partition_ratio.keySet()) {
            Double ratio = partition_ratio.get(e);
            int v = Math.max((int) (ratio * range), 1);//run every batch times then turn.
            for (int i = 0; i < v; i++) {
                extendedTargetId.add(e);
            }
        }
    }

    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param bid
     * @param output
     * @return
     */
    @Override
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {

        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }


    @Override
    public int emit(Meta meta, String streamId, long bid, Object output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
//		int target = extendedTargetId.GetAndUpdate(meta.index++);
        offer(meta.src_id, extendedTargetId.get(meta.index++), streamId, bid, output);
        return 0;//target not in use anyway.
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, int targetId, Object... output) throws InterruptedException {
        force_offer(meta.src_id, targetId, streamId, bid, output);
        return targetId;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        force_offer(meta.src_id, target, streamId, msg_id, bid, output);
        return target;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {//here
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }

    @Override
    public int force_emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }


    @Override
    public int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer(meta.src_id, target, streamId, bid, deviceID, nextDouble, movingAvergeInstant);
        return target;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {

        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }

    @Override
    public int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer(meta.src_id, target, streamId, bid, key, value);
        return target;
    }

    @Override
    public int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer(meta.src_id, target, streamId, key, value);
        return target;
    }

    @Override
    public int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer(meta.src_id, target, streamId, key, value, bid, TimeStamp);
        return target;
    }

    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param bid
     * @param gap
     * @param tuple
     * @return
     */
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... tuple) {

        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer_inorder(meta.src_id, target, streamId, bid, gap, tuple);
        return target;
    }

    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] tuple) {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer_inorder(meta.src_id, target, streamId, bid, gap, tuple);
        return target;
    }

    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer_inorder(meta.src_id, target, streamId, bid, gap, tuple);
        return target;
    }

    @Override
    public int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer_inorder_single(meta.src_id, target, streamId, bid, gap, tuple);
        return target;
    }

    @Override
    public int emit_inorder_push(Meta meta, String streamId, long bid, LinkedList<Long> gap) throws InterruptedException {
        return 0;
    }


    @Override
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }

    @Override
    public int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }

    @Override
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }

    /**
     * partition according to partition ratio.
     *
     * @param meta
     * @param streamId
     * @param output
     * @return
     */
    public int emit_nowait(Meta meta, String streamId, Object... output) {
        //TODO: % is too slow, need some way to implement faster round-robin.
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }

    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        //TODO: % is too slow, need some way to implement faster round-robin.
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        try_offer(meta.src_id, target, streamId, key, value);
        return target;
    }


    public int emit_nowait(Meta meta, String streamId, char[] output) {
        //TODO: % is too slow, need some way to implement faster round-robin.
        if (meta.index == extendedTargetId.size()) {
            meta.index = 0;
        }
        int target = extendedTargetId.get(meta.index++);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }
}
