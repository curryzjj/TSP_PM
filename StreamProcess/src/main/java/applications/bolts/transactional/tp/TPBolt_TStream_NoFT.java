package applications.bolts.transactional.tp;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class TPBolt_TStream_NoFT extends TPBolt_TStream{
    private static final long serialVersionUID = 2529061168277309169L;

    public TPBolt_TStream_NoFT(int fid) {
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
                            MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                            break;
                        case "finish":
                            this.markerId = in.getBID();
                            if(TXN_PROCESS()){
                                /* All the data has been executed */
                                MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
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
        int FT = transactionManager.start_evaluate(thread_Id, this.markerId);
        MeasureTools.finishTransaction(this.thread_Id, System.nanoTime());
        boolean transactionSuccess = FT == 0;
        switch (FT){
            case 0:
                MeasureTools.startPostTransaction(thread_Id, System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPostTransaction(thread_Id, System.nanoTime());
                LREvents.clear();
                BUFFER_PROCESS();
                break;
            case 1:
                MeasureTools.Transaction_abort_begin(this.thread_Id, System.nanoTime());
                SyncRegisterUndo();
                transactionSuccess = this.TXN_PROCESS();
                MeasureTools.Transaction_abort_finish(this.thread_Id, System.nanoTime());
                break;
        }
        return transactionSuccess;
    }
}
