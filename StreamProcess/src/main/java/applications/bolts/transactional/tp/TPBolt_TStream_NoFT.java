package applications.bolts.transactional.tp;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class TPBolt_TStream_NoFT extends TPBolt_TStream{
    public TPBolt_TStream_NoFT(int fid) {
        super(fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if (in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(), this.executor)){
                if(in.getMarker().getValue().equals("finish")){
                    TXN_PROCESS();
                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                    this.context.stop_running();
                } else {
                    TXN_PROCESS();
                    //forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
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
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        transactionManager.start_evaluate(thread_Id,this.markerId);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        REQUEST_CORE();
        REQUEST_POST();
        LREvents.clear();//clear stored events.
        BUFFER_PROCESS();
        return true;
    }
}
