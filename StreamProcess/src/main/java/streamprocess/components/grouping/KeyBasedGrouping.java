package streamprocess.components.grouping;

public class KeyBasedGrouping extends Grouping{
    KeyBasedGrouping(String componentId, String streamID) {
        super(componentId, streamID);
    }

    public KeyBasedGrouping(String componentId) {
        super(componentId);
    }
}
