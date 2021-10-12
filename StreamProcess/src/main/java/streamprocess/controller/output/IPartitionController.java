package streamprocess.controller.output;

import System.util.DataTypes.StreamValues;
import streamprocess.execution.runtime.collector.Meta;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Every exectuor owns a partitionController, who owns a output queue for each downstream executor.
 * This output queue is shared by downstream executor through receiving_queue memory struct
 */

public interface IPartitionController extends Serializable {
    int emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException;

    int emit(Meta meta, String streamId, long bid, Object output) throws InterruptedException;

    int force_emit(Meta meta, String streamId, long bid, Object... output) throws InterruptedException;

    int force_emit(Meta meta, String streamId, long[] bid, long msg_id, Object... output) throws InterruptedException;


    int force_emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException;

    int force_emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException;

    int emit(Meta meta, String streamId, long bid, StreamValues output) throws InterruptedException;

    int emit(Meta meta, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException;

    int emit(Meta meta, String streamId, long bid, char[] output) throws InterruptedException;

    int emit(Meta meta, String streamId, long bid, char[] key, long value) throws InterruptedException;

    int emit(Meta meta, String streamId, char[] key, long value) throws InterruptedException;

    int emit(Meta meta, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException;


    int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, Object... output) throws InterruptedException;

    int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, char[] output) throws InterruptedException;


    int emit_inorder(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException;

    int emit_inorder_single(Meta meta, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException;

    int emit_inorder_push(Meta meta, String streamId, long bid, LinkedList<Long> gap) throws InterruptedException;

    int emit_bid(Meta meta, String streamId, Object... output) throws InterruptedException;

    int emit_bid(Meta meta, String streamId, StreamValues output) throws InterruptedException;

    int emit_bid(Meta meta, String streamId, char[] output) throws InterruptedException;

//	int emit_marked(String streamId, List<Object> output, long timestamp, long bid);
//
//	int emit_marked(String streamId, List<Object> output, Marker marker);

    int emit_nowait(Meta meta, String streamId, Object... output);

    int emit_nowait(Meta meta, String streamId, char[] key, long value);


    int emit_nowait(Meta meta, String streamId, char[] output) throws InterruptedException;

    int marker_boardcast(Meta meta, String streamId, long bid, Marker marker) throws InterruptedException;

    int create_marker_single(Meta meta, String streamId, long timestamp, long bid, int myiteration);

    int create_marker_boardcast(Meta meta, String streamId, long timestamp, long bid, int myiteration) throws InterruptedException;
//    int emit(TupleImpl output, TupleImpl input);
}
