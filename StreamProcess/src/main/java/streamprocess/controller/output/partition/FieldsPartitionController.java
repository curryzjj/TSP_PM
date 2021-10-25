package streamprocess.controller.output.partition;

import System.util.Configuration;
import applications.DataTypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.PartitionController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.Meta;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.TupleUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

public class FieldsPartitionController extends PartitionController {
    private static Logger LOG = LoggerFactory.getLogger(FieldsPartitionController.class);
    protected final Fields input_fields;
    final Fields output_fields;
    private final int targetTasksize;
    public FieldsPartitionController(TopologyComponent operator, TopologyComponent childOP, HashMap<Integer, ExecutionNode> executionNodeHashMap,
                                     Fields output_fields, Fields input_fields, int batch_size, ExecutionNode executor, boolean common, boolean profile, Configuration conf) {
        super(operator, childOP, executionNodeHashMap, batch_size, executor, common, LOG, profile, conf);
        Set<Integer> setID = super.getDownExecutor_list().keySet();
        targetTasksize = setID.size();
        targetTasks = setID.toArray(new Integer[setID.size()]);
        this.output_fields = output_fields;
        this.input_fields = input_fields;
    }
    //chooseTasks method
    public int chooseTasks(Object... values) {
        int targetTaskIndex = TupleUtils.chooseTaskIndex(output_fields.select(input_fields, values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(Object values) {
        int targetTaskIndex = TupleUtils.chooseTaskIndex(output_fields.select(input_fields, values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(StreamValues values) {
        int targetTaskIndex = TupleUtils.chooseTaskIndex(output_fields.select(input_fields, values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(char[] values) {
        int targetTaskIndex = TupleUtils.chooseTaskIndex(Arrays.hashCode(values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    public int chooseTasks(int values) {
        int targetTaskIndex = TupleUtils.chooseTaskIndex(Integer.hashCode(values), targetTasksize);
        return targetTasks[targetTaskIndex];
    }
    //emit_bid
    @Override
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {

        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }

    @Override
    public int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException {
        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }

    @Override
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        offer_bid(meta.src_id, target, streamId, output);
        return target;
    }
    //force_emit
    @Override
    public int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, msg_id, bid, output);
        return target;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        int target = chooseTasks(output);
        force_offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    //emit
    @Override
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {

        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, Object output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {

        int target = chooseTasks(deviceID);
        offer(meta.src_id, target, streamId, bid, deviceID, nextDouble, movingAvergeInstant);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        int target = chooseTasks(output);
        offer(meta.src_id, target, streamId, bid, output);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException {
        int target = chooseTasks(key);
        offer(meta.src_id, target, streamId, bid, key, value);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException {
        int target = chooseTasks(key);
        offer(meta.src_id, target, streamId, key, value);
        return target;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        int target = chooseTasks(key);
        offer(meta.src_id, target, streamId, key, value, bid, TimeStamp);
        return target;
    }
    //emit_inorder
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) {

        int target = chooseTasks(output);
        offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] output) {
        int target = chooseTasks(output);
        offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues output) {
        int target = chooseTasks(output);
        offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }

    @Override
    public int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues output) {
        int target = chooseTasks(output);
        offer_inorder_single(meta.src_id, target, streamId, bid, gap, output);
        return target;
    }

    @Override
    public int emit_inorder_push(Meta meta, String streamId, long bid, LinkedList<Long> gap) throws InterruptedException {
        return 0;
    }
    //emit_nowait
    @Override
    public int emit_nowait(Meta meta, String streamId, Object... output) {

        int target = chooseTasks(output);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {

        int target = chooseTasks(key);
        try_offer(meta.src_id, target, streamId, key, value);
        return target;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] output) {

        int target = chooseTasks(output);
        try_offer(meta.src_id, target, streamId, output);
        return target;
    }
}
