package streamprocess.components.grouping;

public class ShuffleGrouping extends Grouping{

    ShuffleGrouping(String componentId, String streamID) {
        super(componentId, streamID);
    }

    public ShuffleGrouping(String componentId) {
        super(componentId);
    }
}
