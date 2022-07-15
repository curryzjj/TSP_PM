package streamprocess.execution.runtime.collector;

import System.util.DataTypes.StreamValues;
import System.util.OsUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.components.topology.TopologyContext;
import streamprocess.controller.output.OutputController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.FailureFlag;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.util.Set;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.NUM_EVENTS;
import static UserApplications.CONTROL.enable_debug;

/**
 * outputCollector is unique to each executor
 * emit the StreamValues by the OutputController
 */
public class OutputCollector<T> {
    private static final Logger LOG = LoggerFactory.getLogger(OutputCollector.class);
    public final OutputController sc;
    private final ExecutionNode executor;
    private final MetaGroup meta;
    private boolean no_wait = false;
    public OutputCollector(ExecutionNode executor, TopologyContext context){
        int taskId = context.getThisTaskIndex();
        this.executor = executor;
        this.sc = executor.getController();
        this.meta = new MetaGroup(taskId);
        for(TopologyComponent childrenOP:this.executor.getChildren().keySet()){
            this.meta.put(childrenOP, new Meta(taskId));
        }
        if (OsUtils.isMac()) {
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        } else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }
        OsUtils.configLOG(LOG);
    }
    public void emit_nowait(char[] data) throws InterruptedException{
        emit_nowait(DEFAULT_STREAM_ID,data);
    }
    private void emit_nowait(String streamId, char[] str) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        assert str != null && sc != null;
        sc.emitOnStream_nowait(meta, streamId, str);
    }
    private void emit(String streamId,char[] data) throws InterruptedException{
        assert data !=null && sc!=null;
        sc.emitOnStream(meta,streamId,data);
    }
    private void emit(String streamId,char[] data,long bid) throws InterruptedException{
        assert data !=null && sc!=null;
        sc.emitOnStream(meta,streamId,bid,data);
    }
    public void emit(char[] data) throws InterruptedException{
        emit(DEFAULT_STREAM_ID,data);
    }
    public void emit(char[] data,long bid) throws InterruptedException{
        emit(DEFAULT_STREAM_ID,data,bid);
    }
    public void emit(long bid, Object... values) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, bid, values);
    }
    public void emit(String streamId, long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, bid, data);
    }
    public void emit(String streamId, long bid, Object data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, bid, data);
    }
    //force_emit
    public void force_emit(char[] values) throws InterruptedException {
        force_emit(DEFAULT_STREAM_ID, values);
    }
    public void force_emit(String streamId, char[] data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, data);
    }
    public void force_emit(long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, DEFAULT_STREAM_ID, bid, data);
    }
    //broadcast failureFlag
    public void broadcast_failureFlag(long bid, FailureFlag flag) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        sc.failureFlag_boardcast(meta, bid, flag);
    }
    //broadcast marker
    public void broadcast_marker(long bid, Marker marker) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        sc.marker_boardcast(meta, bid, marker);
    }

    public void broadcast_marker(String streamId, long bid, Marker marker) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        sc.marker_boardcast(meta, streamId, bid, marker);
    }

    public void single_marker(String streamId, long bid,int targetId, Marker marker) throws InterruptedException{
        if (executor.isLeafNode()){
            return;
        }
        sc.marker_single(meta,streamId,bid,targetId,marker);
    }
    //create marker
    public void create_marker_single(long boardcast_time, String streamId, long bid, int myiteration){
        sc.create_marker_single(meta, boardcast_time, streamId, bid, myiteration);
    }
    public void create_marker_boardcast(long boardcast_time, String streamId, long bid, int myiteration, String msg) throws InterruptedException {
        sc.create_marker_boardcast(meta, boardcast_time, streamId, bid, myiteration,msg);
    }
    //emit single
    public void emit_single(String streamId, long[] bid, long msg_id, Object data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, msg_id, data);
    }
    public void emit_single(String streamId, long bid, StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, data);
    }
    public int emit_single(String streamId, long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        return sc.force_emitOnStream(meta, streamId, bid, data);
    }
    //emit single to target executor
    public void emit_single_ID(String streamId, int targetId, long bid,Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream_ID(meta, streamId,targetId,bid,data);
    }
    public void emit_single(long bid, Set<Integer> keys) throws InterruptedException {
        emit_single(DEFAULT_STREAM_ID, bid, keys);
    }
    //ack marker
    public void ack(Tuple input) {
        final int executorID = executor.getExecutorID();
        if (enable_debug)
            LOG.info(executor.getOP_full() + " is giving acknowledgement for marker:" + input.getFailureFlag().msgId + " to " + input.getSourceComponent());
        final ExecutionNode src = input.getContext().getExecutor(input.getSourceTask());
        if (input.getBID() != NUM_EVENTS)
            src.op.callback(executorID, input);
    }
    public void broadcast_ack(Tuple input) {
        final int executorID = this.executor.getExecutorID();
        for (TopologyComponent op : executor.getParents_keySet()) {
            for (ExecutionNode src : op.getExecutorList()) {
                src.op.callback(executorID, input);
            }
        }
    }
    public void cleanAll() {
        sc.cleanAll();
    }

    public void clean(String streamId, int targetId){
        sc.clean(streamId,targetId);
    }
}
