package streamprocess.execution.runtime.tuple.msgs;

import streamprocess.execution.runtime.tuple.Message;

import java.util.Arrays;
import java.util.LinkedList;

public class GeneralMsg<T> extends Message {
     public final LinkedList<T> values=new LinkedList<>();
     public GeneralMsg(String streamId,T... values){
         super(streamId,values.length);
         this.values.addAll(Arrays.asList(values));
     }
    public GeneralMsg(String streamId, T values) {
        super(streamId, 1);
        this.values.add(values);
    }
    @Override
    public Object getValue() {
        return values;
    }

    @Override
    public T getValue(int index_fields) {
        return values.get(index_fields);
    }

    @Override
    public boolean isMarker() {
        return false;
    }
    @Override
    public boolean isFailureFlag() {
        return false;
    }
    @Override
    public Marker getMarker() {
        return null;
    }
}
