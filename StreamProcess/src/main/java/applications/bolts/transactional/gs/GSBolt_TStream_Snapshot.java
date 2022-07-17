package applications.bolts.transactional.gs;

import System.measure.MeasureTools;
import UserApplications.CONTROL;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.FailureFlag;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.*;

public class GSBolt_TStream_Snapshot extends GSBolt_TStream{
    private static final long serialVersionUID = 6126484047591969728L;
    public GSBolt_TStream_Snapshot(int fid) {
        super(fid);
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(CONTROL.failureFlag.get()){
            if (this.executor.isFirst_executor()) {
                this.db.getTxnProcessingEngine().mimicFailure(lostPartitionId);
                CONTROL.failureFlagBid.add(in.getBID());
            }
            this.SyncRegisterRecovery();
            this.collector.cleanAll();
            this.EventsHolder.clear();
            for (Queue<Tuple> tuples : bufferedTuples.values()) {
                tuples.clear();
            }
        }else {
            if(in.isMarker()){
                if (status.isMarkerArrived(in.getSourceTask())) {
                    PRE_EXECUTE(in);
                } else {
                    if(status.allMarkerArrived(in.getSourceTask(), this.executor)){
                        switch (in.getMarker().getValue()){
                            case "marker":
                                this.markerId = in.getBID();
                                if (TXN_PROCESS()) {
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                    MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                }
                                break;
                            case "snapshot":
                                this.markerId = in.getBID();
                                this.isSnapshot = true;
                                if(TXN_PROCESS_FT()){
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                    MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                }
                                break;
                            case "finish":
                                this.markerId = in.getBID();
                                if(TXN_PROCESS()){
                                    /* All the data has been executed */
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                    MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                }
                                this.context.stop_running();
                                break;
                            default:
                                throw new IllegalStateException("Unexpected value: " + in.getMarker().getValue());
                        }
                    }
                }
            }else{
                execute_ts_normal(in);
            }
        }
    }

    @Override
    protected boolean TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        int FT = transactionManager.start_evaluate(thread_Id,this.markerId);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        boolean transactionSuccess=FT==0;
        switch (FT){
            case 0:
                this.AsyncRegisterPersist();
                MeasureTools.startPostTransaction(thread_Id, System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPostTransaction(thread_Id, System.nanoTime());
                this.SyncCommitLog();
                EventsHolder.clear();//clear stored events.
                BUFFER_PROCESS();
                break;
            case 1:
                this.SyncRegisterUndo();
                this.AsyncReConstructRequest();
                transactionSuccess = this.TXN_PROCESS_FT();
                break;
        }
        return transactionSuccess;
    }

    @Override
    protected boolean TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        int FT = transactionManager.start_evaluate(thread_Id,this.markerId);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        boolean transactionSuccess = FT == 0;
        switch (FT){
            case 0:
                MeasureTools.startPostTransaction(thread_Id, System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPostTransaction(thread_Id, System.nanoTime());
                EventsHolder.clear();
                BUFFER_PROCESS();
                break;
            case 1:
                this.SyncRegisterUndo();
                this.AsyncReConstructRequest();
                transactionSuccess = this.TXN_PROCESS();
                break;
        }
        return transactionSuccess;
    }
}
