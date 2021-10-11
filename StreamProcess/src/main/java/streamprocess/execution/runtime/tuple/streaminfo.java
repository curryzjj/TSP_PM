package streamprocess.execution.runtime.tuple;

import java.io.Serializable;

public class streaminfo implements Serializable {
    private Fields fields;
    public streaminfo(Fields fields,boolean direct){
        this.fields=fields;
    }
    public Fields getFields(){
        return fields;
    }

    public void setFields(Fields fields) {
        this.fields = fields;
    }
}
