package engine.transaction;

import engine.table.tableRecords.SchemaRecordRef;

public class TxnContext {
    public final int thread_Id;
    private final int fid;
    private final long bid;
    private final String thisOpId;
    public long[] partition_bid;
    public int pid;
    public long index_time;
    public long ts_allocation;
    public boolean is_retry;
    public boolean success;
    private double[] index_time_;
    private int txn_type_;
    private boolean is_read_only_;
    private boolean is_dependent_;
    private boolean is_adhoc_;
    private SchemaRecordRef record_ref;


    public TxnContext(int thread_Id, int fid, long bid) {
        this.thread_Id=thread_Id;
        this.thisOpId = null;
        this.fid = fid;
        this.bid = bid;
        is_adhoc_ = false;
        is_read_only_ = false;
        is_dependent_ = false;
        is_retry= false;
        record_ref=null;
    }
    //TODO:some constroctor function

    public String getThisOpId() {
        return thisOpId;
    }
    public int getFID() {
        return fid;
    }

    public long getBID() {
        return bid;
    }

}
