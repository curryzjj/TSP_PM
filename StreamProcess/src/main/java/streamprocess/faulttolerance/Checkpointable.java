package streamprocess.faulttolerance;

import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.util.concurrent.BrokenBarrierException;

public interface Checkpointable {
    boolean checkpoint(int counter) throws InterruptedException, BrokenBarrierException;
    void forward_checkpoint(int sourceId,long bid, Marker marker,String msg) throws InterruptedException;
    void forward_checkpoint_single(int sourceTask,String streamId,long bid,Marker marker) throws InterruptedException;
    void forward_checkpoint(int sourceTask,String streamId,long bid,Marker marker, String msg) throws InterruptedException;

    void ack_checkpoint(Marker marker);
    void earlier_ack_checkpoint(Marker marker);
}
