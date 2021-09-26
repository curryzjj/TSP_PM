package streamprocess.components.grouping;

import streamprocess.execution.runtime.tuple.Fields;

import java.io.Serializable;

import static System.Constants.DEFAULT_STREAM_ID;

/**
 * use in the outputController
 */

public abstract class Grouping implements Serializable {
    //source operator of this Grouping
    private final String componentId;
    //source stream if this Grouping
    private String streamID=DEFAULT_STREAM_ID;

    Grouping(String componentId, String streamID) {
        this.componentId = componentId;
        this.streamID = streamID;
    }
    Grouping(String componentId){
        this.componentId=componentId;
    }
    public String getStreamID() {
        return streamID;
    }
    public String getComponentId() {
        return componentId;
    }
    public Fields getFields(){return null;}
    //use in the ExecutionGraph to set the outputController
    public boolean isShuffle() {
        return this instanceof ShuffleGrouping;
    }
    public boolean isMarkerShuffle() {
        return this instanceof MakerShuffleGrouping;
    }
    public boolean isFields() {
        return this instanceof FieldsGrouping;
    }
    public boolean isAll() {
        return this instanceof AllGrouping;
    }
    public boolean isPartial() {
        return this instanceof PartialKeyGrouping;
    }
    public boolean isGlobal() {
        return this instanceof GlobalGrouping;
    }

}
