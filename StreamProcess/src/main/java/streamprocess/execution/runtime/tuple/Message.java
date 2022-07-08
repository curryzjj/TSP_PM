package streamprocess.execution.runtime.tuple;

import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.Serializable;

public abstract class Message implements Serializable {
    private static final long serialVersionUID = -6235214610609064555L;
    public final String streamId;
    final int field_size;
    protected Message(String streamId,int field_size){
        this.field_size=field_size;
        this.streamId=streamId;
    }
    public abstract Object getValue();
    public abstract Object getValue(int index_fields);
    public String getStreamId(){return streamId;}
    public abstract boolean isMarker();
    public abstract boolean isFailureFlag();
    public abstract Marker getMarker();
}
