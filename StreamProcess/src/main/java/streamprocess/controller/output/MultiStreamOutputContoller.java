package streamprocess.controller.output;

import System.util.DataTypes.StreamValues;
import streamprocess.components.topology.MultiStreamComponent;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.collector.MetaGroup;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.execution.runtime.tuple.streaminfo;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

import System.Constants;

public class MultiStreamOutputContoller extends OutputController{
    private final HashMap<String, HashMap<String, PartitionController>> PClist;
    private HashMap<String, PartitionController[]> collections; //<output_streamId,PC[]>> for each stream
    private HashMap<String, Integer> counter;
    public MultiStreamOutputContoller(MultiStreamComponent op,HashMap<String,HashMap<String,PartitionController>> PClist){
        super();
        HashMap<String, streaminfo> output_streams=op.getOutput_streams();
        this.PClist=PClist; //<output_streamId,<DownOpId,PC>> for each executor
        collections=new HashMap<>();
        counter=new HashMap<>();
        for(String streamId:output_streams.keySet()){
            PartitionController[] PartitionControllers=new PartitionController[getPartitionController(streamId).size()];
            for(int i=0;i<getPartitionController(streamId).size();i++){
                PartitionControllers[i]=getPartitionController(streamId).iterator().next();
            }
            counter.put(streamId,0);
            collections.put(streamId,PartitionControllers);
        }
    }

    @Override
    public PartitionController getPartitionController(String streamId, String boltID) {
        return PClist.get(streamId).get(boltID);
    }

    @Override
    public Collection<PartitionController> getPartitionController() {
        return PClist.get(Constants.DEFAULT_STREAM_ID).values();
    }

    @Override
    public Collection<PartitionController> getPartitionController(String StreamId) {
        return PClist.get(StreamId).values();
    }

    @Override
    public boolean isEmpty() {
        for(String stream:PClist.keySet()){
            for(String op:PClist.get(stream).keySet()){
                if(!PClist.get(stream).get(op).isEmpty()){
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void allocatequeue(boolean linked, int desired_elements_epoch_per_core) {
        for(String stream:PClist.keySet()){
            for(String op:PClist.get(stream).keySet()){
                PClist.get(stream).get(op).allocate_queue(linked,desired_elements_epoch_per_core);
            }
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object... output) throws InterruptedException {
        PartitionController[] it=collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, bid, output);
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object data) throws InterruptedException {

    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {

    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException {//here
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.force_emit(MetaGroup.get(p.childOP), streamId, -1, data);//default bid is -1
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] data, long bid, long timestamp) throws InterruptedException {

    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] key, long value) throws InterruptedException {

    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {

    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, char[] data) throws InterruptedException {

    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, char[] key, long value) throws InterruptedException {

    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, StreamValues data) throws InterruptedException {

    }

    @Override
    public void emitOnStream_bid(MetaGroup MetaGroup, String streamId, Object... output) throws InterruptedException {

    }

    @Override
    public void emitOnStream_bid(MetaGroup MetaGroup, String streamId, Object output) throws InterruptedException {

    }

    @Override
    public void emitOnStream_bid(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException {

    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.force_emit(MetaGroup.get(p.childOP), streamId, -1, data);//default bid is -1
        }
    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, long bid, StreamValues data) throws InterruptedException {

    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object... data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.force_emit(MetaGroup.get(p.childOP), streamId, bid, data);
        }
    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, long[] bid, long msg_id, Object... data) throws InterruptedException {

    }

    @Override
    public void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, Object... data) throws InterruptedException {

    }

    @Override
    public void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, char[] data) throws InterruptedException {

    }

    @Override
    public void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {

    }

    @Override
    public void emitOnStream_inorder_single(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {

    }

    @Override
    public void emitOnStream_inorder_push(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap) throws InterruptedException {

    }

    @Override
    public void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, Object... data) {

    }

    @Override
    public void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, char[] key, long value) {

    }

    @Override
    public void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException {

    }

    @Override
    public void create_marker_single(MetaGroup meta, long boardcast_time, String streamId, long bid, int myiteration) {

    }

    @Override
    public void create_marker_boardcast(MetaGroup MetaGroup, long timestamp, long bid, int myitration) throws InterruptedException {

    }

    @Override
    public void create_marker_boardcast(MetaGroup meta, long timestamp, String streamId, long bid, int myiteration) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.create_marker_boardcast(meta.get(p.childOP), streamId, timestamp, bid, myiteration,"");
        }
    }

    @Override
    public void marker_boardcast(MetaGroup MetaGroup, long bid, Marker marker) throws InterruptedException {
        for (String streamId : PClist.keySet()) {
            PartitionController[] it = collections.get(streamId);
            for (int i = 0; i < it.length; i++) {
                PartitionController p = it[i];
                p.marker_boardcast(MetaGroup.get(p.childOP), streamId, bid, marker);
            }
        }
    }

    @Override
    public void marker_boardcast(MetaGroup MetaGroup, String streamId, long bid, Marker marker) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.marker_boardcast(MetaGroup.get(p.childOP), streamId, bid, marker);
        }
    }

    @Override
    public void setContext(int executorID, TopologyContext context) {

    }

    @Override
    public long getBID(String streamId) {
        return 0;
    }
}
