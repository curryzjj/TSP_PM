package streamprocess.controller.output.partition;

import System.util.Configuration;
import System.util.DataTypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.PartitionController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.Meta;
import streamprocess.execution.runtime.tuple.Fields;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

public class AllPartitionController extends PartitionController {
    private static final Logger LOG= LoggerFactory.getLogger(AllPartitionController.class);
    private final int downExecutor_size;
    private Fields fields;
    public AllPartitionController(TopologyComponent operator, TopologyComponent childOP, HashMap<Integer, ExecutionNode> executionNodeHashMap,
                                  int batch, ExecutionNode executor, boolean common, boolean profile, Configuration conf){
        super(operator,childOP,executionNodeHashMap,batch,executor,common,LOG,profile,conf);
        Set<Integer> setID=super.getDownExecutor_list().keySet();
        downExecutor_size=setID.size();
        targetTasks=setID.toArray(new Integer[setID.size()]);
        this.fields=fields;
    }
    //force_emit uses the force_offer method in the PartitionController
    @Override
    public int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        for (int target : targetTasks) {
            force_offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException {
        for (int target : targetTasks) {
            force_offer(meta.src_id, target, streamId, msg_id, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        throw new UnsupportedOperationException();
    }
    @Override
    public int force_emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        throw new UnsupportedOperationException();
    }
    //emit uses the offer method in the PartitionController
    @Override
    public int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, Object output) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException {
        for(int target:targetTasks){
            offer(meta.src_id,target,streamId,bid,output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, deviceID, nextDouble, movingAvergeInstant);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, bid, key, value);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, key, value);
        }
        return downExecutor_size;
    }
    @Override
    public int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        for (int target : targetTasks) {
            offer(meta.src_id, target, streamId, key, value, bid, TimeStamp);
        }
        return downExecutor_size;
    }
    //end
    //emit_inorder uses the offer_inorder method in the PartitionController
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) throws InterruptedException {
        for(int target:targetTasks){
            offer_inorder(meta.src_id,target,streamId,bid,gap,output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] output) throws InterruptedException {
        for (int target : targetTasks) {
            offer_inorder(meta.src_id, target, streamId, bid, gap, output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        for (int target : targetTasks) {
            offer_inorder(meta.src_id, target, streamId, bid, gap, tuple);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        for (int target : targetTasks) {
            offer_inorder_single(meta.src_id, target, streamId, bid, gap, tuple);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_inorder_push(Meta meta, String streamId, long bid, LinkedList<Long> gap) throws InterruptedException {
        for (int target : targetTasks) {
            offer_inorder_push(meta.src_id, target, streamId, bid, gap);
        }
        return -1;
    }
    //emit_bid uses the offer_bid in the PartitionController
    @Override
    public int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException {
        for(int target:targetTasks){
            offer_bid(meta.src_id,target,streamId,output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException {
        for(int target:targetTasks){
            offer_bid(meta.src_id,target,streamId,output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException {
        for(int target:targetTasks){
            offer_bid(meta.src_id,target,streamId,output);
        }
        return downExecutor_size;
    }
    //end
    //emit_nowait uses the try_offer in the PartitionController
    @Override
    public int emit_nowait(Meta meta, String streamId, Object... output) {
        for(int target:targetTasks){
            try_offer(meta.src_id,target,streamId,output);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] key, long value) {
        for (int target : targetTasks) {
            try_offer(meta.src_id, target, streamId, key, value);
        }
        return downExecutor_size;
    }
    @Override
    public int emit_nowait(Meta meta, String streamId, char[] output) throws InterruptedException {
        for (int target : targetTasks) {
            try_offer(meta.src_id, target, streamId, output);
        }
        return downExecutor_size;
    }

    //end
}
