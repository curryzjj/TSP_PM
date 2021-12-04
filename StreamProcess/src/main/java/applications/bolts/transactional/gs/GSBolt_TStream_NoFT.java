package applications.bolts.transactional.gs;

import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class GSBolt_TStream_NoFT extends GSBolt_TStream{
    public GSBolt_TStream_NoFT(int fid) {
        super(fid);
        this.configPrefix="gstxn";
        status=new Status();
        this.setStateful();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(in.isMarker()){
            if (status.allMarkerArrived(in.getSourceTask(),this.executor)){
                this.collector.ack(in,in.getMarker());
                if(EventsHolder.size()!=0){
                    TXN_PROCESS();
                }
                forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
            }
        }else {
            execute_ts_normal(in);
        }
    }

    @Override
    protected boolean TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        return false;
    }

    @Override
    protected boolean TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        transactionManager.start_evaluate(thread_Id,this.fid);
        REQUEST_CORE();
        REQUEST_POST();
        EventsHolder.clear();//clear stored events.
        BUFFER_PROCESS();
        bufferedTuple.clear();
        return true;
    }
}
