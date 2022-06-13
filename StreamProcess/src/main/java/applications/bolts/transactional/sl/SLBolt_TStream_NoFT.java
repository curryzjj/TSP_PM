package applications.bolts.transactional.sl;

import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class SLBolt_TStream_NoFT extends SLBolt_TStream {
    public SLBolt_TStream_NoFT(int fid){
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
        REQUEST_CORE();
        REQUEST_POST();
        EventsHolder.clear();
        BUFFER_PROCESS();
        return true;
    }
}
