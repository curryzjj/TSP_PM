package applications.bolts.transactional.sl;

import System.measure.MeasureTools;
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
        if(in.isMarker()){
            if (status.isMarkerArrived(in.getSourceTask())) {
                PRE_EXECUTE(in);
            } else {
                if (status.allMarkerArrived(in.getSourceTask(),this.executor)){
                    switch (in.getMarker().getValue()) {
                        case "marker":
                            this.markerId = in.getBID();
                            TXN_PROCESS();
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                            break;
                        case "finish":
                            this.markerId = in.getBID();
                            if(TXN_PROCESS()){
                                /* All the data has been executed */
                                forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                            }
                            this.context.stop_running();
                            break;
                    }
                }
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
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        transactionManager.start_evaluate(thread_Id,this.markerId);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        REQUEST_CORE();
        REQUEST_POST();
        EventsHolder.clear();
        BUFFER_PROCESS();
        return true;
    }
}
