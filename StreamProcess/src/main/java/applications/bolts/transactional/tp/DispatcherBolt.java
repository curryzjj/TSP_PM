package applications.bolts.transactional.tp;

import applications.DataTypes.PositionReport;
import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.checkpoint.Checkpointable;
import streamprocess.checkpoint.Status;
import streamprocess.components.operators.base.filterBolt;
import streamprocess.execution.runtime.tuple.OutputFieldsDeclarer;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;

import static UserApplications.constants.TP_TxnConstants.Stream.POSITION_REPORTS_STREAM_ID;

public class DispatcherBolt extends filterBolt implements Checkpointable {
    private static final Logger LOG= LoggerFactory.getLogger(DispatcherBolt.class);
    public DispatcherBolt(){
        super(LOG,new HashMap<>());
        status=new Status();
        this.output_selectivity.put(POSITION_REPORTS_STREAM_ID,0.9885696197046802);//what this for???
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        long bid=in.getBID();
        if(in.isMarker()){
            forward_checkpoint(in.getSourceTask(),bid,in.getMarker(),in.getMarker().getValue());
            ack_checkpoint(in.getMarker());
        }else{
            String raw = null;
            try {
                raw = in.getString(0);
            } catch (Exception e) {
                System.nanoTime();
            }
            String[] token = raw.split(" ");
            Short time = Short.parseShort(token[0]);
            Integer vid = Integer.parseInt(token[1]);
            assert (time.shortValue() == Short.parseShort(token[0]));
            this.collector.emit_single(POSITION_REPORTS_STREAM_ID,bid,new PositionReport(
                    time,
                    vid,
                    Integer.parseInt(token[2]), // speed
                    Integer.parseInt(token[3]), // xway
                    Short.parseShort(token[4]), // lane
                    Short.parseShort(token[5]), // direction
                    Short.parseShort(token[6]), // segment
                    Integer.parseInt(token[7])//position
            ));
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {//what this for???
        declarer.declareStream(POSITION_REPORTS_STREAM_ID,PositionReport.getSchema());
    }
    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker,String msg) throws InterruptedException {
        this.collector.broadcast_marker(bid, marker);
    }

    @Override
    public boolean checkpoint(int counter) throws InterruptedException, BrokenBarrierException {
        return false;
    }
    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException {

    }
    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker,String msg) throws InterruptedException {

    }
    @Override
    public void ack_checkpoint(Marker marker) {
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }
    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }
}
