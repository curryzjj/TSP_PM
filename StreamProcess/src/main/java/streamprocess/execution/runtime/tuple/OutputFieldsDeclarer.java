package streamprocess.execution.runtime.tuple;

import java.util.HashMap;

import static System.Constants.DEFAULT_STREAM_ID;

public class OutputFieldsDeclarer {
    private final HashMap<String,streaminfo> _fields=new HashMap<>();
    public void declare(Fields fields){
        declare(false,fields);
    }
    private void declare(boolean direct,Fields fields){
        declareStream(DEFAULT_STREAM_ID,direct,fields);
    }
    public void declareStream(String streamId,Fields fields){
        declareStream(streamId,false,fields);
    }
    public void declareStream(String streamId,boolean direct,Fields fields){
        if(_fields.containsKey(streamId)){
            throw new IllegalArgumentException("Fields for"+streamId+"already set");
        }
        _fields.put(streamId,new streaminfo(fields,direct));
    }
    public HashMap<String, streaminfo> getFieldsDeclaration() {
        return _fields;
    }
}
