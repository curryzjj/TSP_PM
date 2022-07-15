package streamprocess.execution.runtime.tuple.msgs;

import streamprocess.execution.runtime.tuple.Message;

public class FailureFlag extends Message {
    private static final long serialVersionUID = 3519987065470280703L;
    private final int failedPartitionId;
    private final long failureTime;
    public final long msgId;

    public FailureFlag(String streamId, long timeStamp, long msgId, int failedPartitionId) {
        super(streamId, 0);
        this.failedPartitionId = failedPartitionId;
        this.msgId = msgId;
        this.failureTime = timeStamp;
    }

    @Override
    public Object getValue() {
        return failedPartitionId;
    }
    @Override
    public Object getValue(int index_fields) {
        return null;
    }

    @Override
    public boolean isMarker() {
        return false;
    }

    @Override
    public boolean isFailureFlag() {
        return true;
    }
    @Override
    public Marker getMarker() {
        return null;
    }
}
