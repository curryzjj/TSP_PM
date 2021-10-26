package System.util.DataTypes;

import java.util.ArrayList;

public class StreamValues<E> extends ArrayList<E> {
    private Object messageId;
    private String streamId="default";
    public StreamValues(){

    }
    public StreamValues(E... vals){
        super(vals.length);
        for(E o:vals){
            add(o);
        }
    }
    public String getStreamId(){
        return streamId;
    }
    public void setStreamId(String streamId){
        this.streamId=streamId;
    }
}
