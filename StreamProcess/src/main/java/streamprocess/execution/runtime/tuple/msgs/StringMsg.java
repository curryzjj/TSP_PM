package streamprocess.execution.runtime.tuple.msgs;

import streamprocess.execution.runtime.tuple.Message;

public class StringMsg extends Message {
    public final char[] str;

    public StringMsg(String streamId, char[] str) {
        super(streamId, 1);
        this.str = str;
    }

    @Override
    public char[] getValue() {
        return str;
    }

    @Override
    public char[] getValue(int index_fields) {
        return str;
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
