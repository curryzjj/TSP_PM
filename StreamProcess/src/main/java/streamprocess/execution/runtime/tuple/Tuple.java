package streamprocess.execution.runtime.tuple;

import streamprocess.components.topology.TopologyContext;

import java.util.Collection;

/**
 * @Idea: Intelligent Brisk.execution.runtime.tuple: By accessing context, every Brisk.execution.runtime.tuple knows the global Brisk.execution condition (in a much cheap way compare to cluster)!
 * It's possible to make each Brisk.execution.runtime.tuple intelligent!!
 */
public class Tuple {
    private final int sourceId;
    private TopologyContext context;
    private long bid;
    private Message message;
    private long[] partition_bid;
    public Tuple(long bid,int sourceId,TopologyContext context,Message message){
        this.bid=bid;
        this.context=context;
        this.message=message;
        this.sourceId=sourceId;
    }
    public Tuple(long bid, long[] p_bid, int sourceId, TopologyContext context, Message message) {
        this.bid = bid;
        this.sourceId = sourceId;
        this.context = context;
        this.message = message;
        partition_bid = p_bid;
    }
    //get private value
    public TopologyContext getContext() {
        return context;
    }
    public long getBID() {
        return bid;
    }
    public long[] getPartitionBID() {
        return partition_bid;
    }
    public int getSourceTask() {
        return sourceId;
    }
    //Gets the id of the component that created this Brisk.execution.runtime.tuple.
    public String getSourceComponent() {
        return context.getComponent(sourceId).getId();
    }
    public String getSourceStreamId() {
        return message.streamId;
    }
    //get field
    public Fields getFields() {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId());
    }
    private int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }
    //get something
    public Object getValue(int i) {
        return message.getValue(i);
    }
    public String getString(int i) {
        return new String(getCharArray(i));
    }
    public char[] getCharArray(int index_field) {
        return (char[]) getValue(index_field);
    }
    public int getInt(int i) {
        return (int) getValue(i);
    }
    public boolean getBoolean(int i) {
        return (boolean) getValue(i);
    }
    public Long getLong(int i) {
        return (Long) getValue(i);
    }
    public double getDouble(int i) {
        return (double) getValue(i);
    }
    //get something by the field
    public Object getValueByField(String field) {
        return message.getValue((fieldIndex(field)));
    }
    public Double getDoubleByField(String field) {
        return (Double) getValue(fieldIndex(field));
    }
    public String getStringByField(String field) {
        return (String) getValue(fieldIndex(field));
    }
    public short getShortByField(String field) {
        return (short) getValue(fieldIndex(field));
    }
    public int getIntegerByField(String field) {
        return (int) getValue(fieldIndex(field));
    }
    public long getLongByField(String field) {
        return (long) getValue(fieldIndex(field));
    }
    public boolean getBooleanByField(String field) {
        return (boolean) getValue(fieldIndex(field));
    }
    //public TimestampType getTimestampType(int i) {}
    public boolean isMarker() {
        return this.message.isMarker();
    }
    public Marker getMarker() {
        return (Marker) this.message;
    }

    public Collection getValues() {
        return (Collection) this.getValue(0);
    }

    public Short getShort(int timeIdx) {

        return (short) getValue(timeIdx);
    }
}
