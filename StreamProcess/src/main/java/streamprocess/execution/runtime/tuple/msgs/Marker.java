package streamprocess.execution.runtime.tuple.msgs;

import streamprocess.execution.runtime.tuple.Message;

public class Marker extends Message {
    public final long msgId;//this records the ancestor message id of this message.
    public final long timeStampNano;//
    private final int myiteration;
    private final String message;
    private long acknowledge_time;
    public Marker(String streamId, long timeStamp, long msgId, int myiteration,String message) {
        super(streamId, 0);
        this.timeStampNano = timeStamp;
        this.msgId = msgId;
        this.myiteration = myiteration;
        this.message=message;
    }
    public int getMyiteration() {
        return myiteration;
    }
    @Override
    public String getValue() {
        return message;
    }

    @Override
    public Object getValue(int index_fields) {
        return null;
    }

    @Override
    public boolean isMarker() {
        return true;
    }

    @Override
    public Marker getMarker() {
        return this;
    }
}
