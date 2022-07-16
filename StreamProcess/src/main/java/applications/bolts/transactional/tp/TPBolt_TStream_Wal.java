package applications.bolts.transactional.tp;

import System.measure.MeasureTools;
import UserApplications.CONTROL;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.FailureFlag;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.PARTITION_NUM;
import static UserApplications.CONTROL.rnd;

public class TPBolt_TStream_Wal extends TPBolt_TStream{
    private static final long serialVersionUID = 2082014261100781714L;
    public TPBolt_TStream_Wal(int fid) {
        super(fid);
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(CONTROL.failureFlag.get()){
            if (this.executor.isFirst_executor()) {
                this.db.getTxnProcessingEngine().mimicFailure(rnd.nextInt(PARTITION_NUM));
                CONTROL.failureFlagBid.add(in.getBID());
            }
            this.SyncRegisterRecovery();
            this.collector.cleanAll();
            this.LREvents.clear();
            for (Queue<Tuple> tuples : bufferedTuples.values()) {
                tuples.clear();
            }
        } else {
            if(in.isMarker()){
                if (status.isMarkerArrived(in.getSourceTask())) {
                    PRE_EXECUTE(in);
                } else {
                    if (status.allMarkerArrived(in.getSourceTask(),this.executor)){
                        switch (in.getMarker().getValue()){
                            case "recovery":
                                forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                break;
                            case "marker":
                                this.markerId = in.getBID();
                                if (TXN_PROCESS_FT()){
                                    /* When the wal is completed, the data can be consumed by the outside world */
                                    MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                }
                                break;
                            case "snapshot":
                                this.markerId = in.getBID();
                                this.isSnapshot = true;
                                if (TXN_PROCESS_FT()){
                                    /* When the wal is completed, the data can be consumed by the outside world */
                                    MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                }
                                break;
                            case "finish":
                                this.markerId = in.getBID();
                                if(TXN_PROCESS_FT()){
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
    }

    @Override
    protected boolean TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        int FT = transactionManager.start_evaluate(thread_Id,this.markerId);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        boolean transactionSuccess = FT == 0;
        switch (FT){
            case 0:
                this.AsyncRegisterPersist();
                MeasureTools.startPostTransaction(thread_Id, System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPostTransaction(thread_Id, System.nanoTime());
                this.SyncCommitLog();
                LREvents.clear();//clear stored events.
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
        return true;
    }
}
