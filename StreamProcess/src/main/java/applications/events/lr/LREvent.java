package applications.events.lr;

import applications.DataTypes.PositionReport;
import engine.table.tableRecords.SchemaRecordRef;

public class LREvent {
    private final int tthread;
    private final long bid;
    private long timestamp;
    public int count;
    public double lav;
    private PositionReport posreport;
    public SchemaRecordRef speed_value;
    public SchemaRecordRef count_value;
    /**
     * creating a new LREvent.
     *
     * @param posreport
     * @param tthread
     * @param bid
     */
    public LREvent(PositionReport posreport, int tthread, long bid) {
        this.posreport = posreport;
        this.tthread = tthread;
        this.bid = bid;
        speed_value = new SchemaRecordRef();
        count_value = new SchemaRecordRef();
        this.setTimestamp(posreport.getTime());
    }
    public PositionReport getPOSReport() {
        return posreport;
    }
    public int getPid() {
        return posreport.getSegment() % tthread;//which partition does this input_event belongs to.
    }
    public long getBid() {
        return bid;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public long getTimestamp() {
        return timestamp;
    }
}
