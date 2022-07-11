package streamprocess.faulttolerance.checkpoint;

import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.util.concurrent.BrokenBarrierException;

public interface emitMarker {
    boolean marker() throws InterruptedException, BrokenBarrierException;
    void forward_marker(int sourceId, long bid, Marker marker, String msg) throws InterruptedException;
   // void forward_marker_single(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException;
    void forward_marker(int sourceTask, String streamId, long bid, Marker marker, String msg) throws InterruptedException;
    void ack_Signal(Tuple message);
    void earlier_ack_marker(Marker marker);
}
