package streamprocess.execution.runtime.tuple.msgs;

import streamprocess.execution.runtime.tuple.Message;

public class StringLongMsg extends Message {
    public final char[] str;
    public final long value;

    public StringLongMsg(String streamId, char[] str, long value) {
        super(streamId, 2);
        this.str = str;
        this.value = value;
    }

    @Override
    public Object getValue() {
        return str + "" + value;
    }

    @Override
    public Object getValue(int index_fields) {
        switch (index_fields) {
            case 0:
                return str;
            case 1:
                return value;
            default:
                throw new IndexOutOfBoundsException(String.valueOf(index_fields));
        }
    }

    @Override
    public boolean isMarker() {
        return false;
    }

    @Override
    public Marker getMarker() {
        return null;
    }
}
