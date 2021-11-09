package streamprocess.execution.runtime.tuple;

import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.tuple.msgs.Marker;

public class  JumboTuple implements Comparable<JumboTuple> {//batch tuple
    public final Message[] msg;
    protected final int sourceId;
    private final TopologyContext context;
    public int length;
    private long bid;
    /**
     * used in normal Tuple.
     *
     * @param bid
     * @param sourceId
     * @param context
     * @param message
     */
    public JumboTuple(long bid, int sourceId, TopologyContext context, Message message) {
        this.bid = bid;
        this.sourceId = sourceId;
        this.context = context;
        this.msg = new Message[1];
        this.msg[0] = message;
    }
    public JumboTuple(int sourceId, long bid, int length, TopologyContext context) {
        this.sourceId = sourceId;
        this.length = length;
        this.msg = new Message[length];
        this.context = context;
        this.bid = bid;

    }
    public JumboTuple(int sourceId, long bid, int msg_size, TopologyContext context, Message... msg) {
        this.sourceId = sourceId;
        this.context = context;
        this.length = msg_size;//the actual batch size in this tuple
        this.msg = msg;
        this.bid = bid;
    }
    public JumboTuple(JumboTuple clone) {
        this.bid = clone.bid;
        this.sourceId = clone.sourceId;
        this.length = clone.length;
        this.context = clone.context;
        msg = new Message[length];
        System.arraycopy(clone.msg, 0, msg, 0, length);
    }
    //Gets the id of the component that created this Brisk.execution.runtime.tuple.
    public String getSourceComponent() {
        return context.getComponent(sourceId).getId();
    }
    public String getSourceStreamId(int index_msg) {
        return msg[index_msg].streamId;
    }
    public int getSourceTask() {
        return sourceId;
    }
    //get tuple and field
    public Tuple getTuple(int i) {
        return new Tuple(bid, sourceId, context, msg[i]);
    }
    private int fieldIndex(String field, int index_msg) {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId(index_msg)).fieldIndex(field);
    }
    //get value
    public Object getValue(int index_field, int index_msg) {
        return msg[index_msg].getValue(index_field);
    }
    public String getString(int index_field, int index_msg) {
        return new String(getCharArray(index_field, index_msg));

    }
    public char[] getCharArray(int index_field, int index_msg) {
        return (char[]) msg[index_msg].getValue(index_field);
    }
    public int getInt(int index_field, int index_msg) {
        return (int) getValue(index_field, index_msg);
    }
    public long getLong(int index_field, int index_msg) {
        return (long) getValue(index_field, index_msg);
    }
    public double getDouble(int index_field, int index_msg) {
        return (double) getValue(index_field, index_msg);
    }
    //get value by field
    public Object getValueByField(String field, int index_msg) {
        return msg[index_msg].getValue(fieldIndex(field, index_msg));
    }
    public String getStringByField(String field, int index_msg) {
        return (String) getValueByField(field, index_msg);
    }
    public double getDoubleByField(String field, int index_msg) {
        return (double) getValueByField(field, index_msg);
    }
    public int getIntegerByField(String field, int index_msg) {
        return (int) getValueByField(field, index_msg);
    }
    public long getLongByField(String field, int index_msg) {
        return (long) getValueByField(field, index_msg);
    }
    public boolean getBooleanByField(String field, int index_msg) {
        return (boolean) getValueByField(field, index_msg);
    }
    //get private value
    public long getBID() {
        return bid;
    }
    public Message getMsg(int index_msg) {
        return msg[index_msg];
    }
    public TopologyContext getContext() {
        return context;
    }
    public Marker getMarker(int i) {
        return msg[i].getMarker();
    }
    public void add(Integer p, Message message) {
        msg[p] = message;
    }
    @Override
    public int compareTo(JumboTuple o) {
        return 0;
    }
}
