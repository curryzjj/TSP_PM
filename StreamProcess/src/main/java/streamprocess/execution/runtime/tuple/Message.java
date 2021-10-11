package streamprocess.execution.runtime.tuple;

import java.io.Serializable;

public abstract class Message implements Serializable {
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
    public abstract Marker getMarker();

}
