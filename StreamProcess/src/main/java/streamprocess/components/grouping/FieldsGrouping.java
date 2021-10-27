package streamprocess.components.grouping;

import streamprocess.execution.runtime.tuple.Fields;

public class FieldsGrouping extends Grouping{
    private Fields fields;

   public FieldsGrouping(String componentId, String streamID,Fields fields) {
        super(componentId, streamID);
        this.fields = fields;
    }

    FieldsGrouping(String componentId,Fields fields) {
        super(componentId);
        this.fields = fields;
    }
    public Fields getFields() {
        return fields;
    }
    public void setFields(Fields fields) {
        this.fields = fields;
    }
}
