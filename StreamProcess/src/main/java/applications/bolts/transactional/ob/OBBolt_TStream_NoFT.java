package applications.bolts.transactional.ob;

import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class OBBolt_TStream_NoFT extends OBBolt_TStream{
    public OBBolt_TStream_NoFT(int fid) {
        super(fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if (in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                TXN_PROCESS();
                forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                if(in.getMarker().getValue()=="finish"){
                    this.context.stop_running();
                }
            }
        }else{
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
        REQUEST_REQUEST_CORE();
        REQUEST_POST();
        EventsHolder.clear();
        BUFFER_PROCESS();
        return true;
    }
}
