package streamprocess.components.grouping;

import streamprocess.execution.runtime.tuple.Fields;

public class PartialKeyGrouping extends Grouping{
    private Fields fields;

    PartialKeyGrouping(String componentId, String streamID, Fields fields) {
        super(componentId, streamID);
        this.fields = fields;
    }

    PartialKeyGrouping(String componentId, Fields fields) {
        super(componentId);
        this.fields = fields;
    }

    @Override
    public Fields getFields() {
        return fields;
    }

    public void setFields(Fields fields) {
        this.fields = fields;
    }
}
