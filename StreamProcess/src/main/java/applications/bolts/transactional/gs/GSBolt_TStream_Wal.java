package applications.bolts.transactional.gs;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.FailureFlag;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class  GSBolt_TStream_Wal extends GSBolt_TStream{
    private static final long serialVersionUID = -7735253055437818414L;
    public GSBolt_TStream_Wal(int fid) {
        super(fid);
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if (in.isFailureFlag()) {
            FailureFlag failureFlag = in.getFailureFlag();
            if (this.executor.isFirst_executor()) {
                this.db.getTxnProcessingEngine().getRecoveryRangeId().add((int) failureFlag.getValue());
            }
            this.SyncRegisterRecovery();
            this.collector.cleanAll();
            this.EventsHolder.clear();
            for (Queue<Tuple> tuples : bufferedTuples.values()) {
                tuples.clear();
            }
        } else {
            if(in.isMarker()){
                if (status.isMarkerArrived(in.getSourceTask())) {
                    PRE_EXECUTE(in);
                } else {
                    if (status.allMarkerArrived(in.getSourceTask(),this.executor)){
                        //this.collector.ack(in,in.getMarker());
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
        MeasureTools.startTransaction(thread_Id, System.nanoTime());
        int FT = transactionManager.start_evaluate(thread_Id, this.markerId);
        MeasureTools.finishTransaction(thread_Id, System.nanoTime());
        boolean transactionSuccess = FT == 0;
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
      return true;
    }
}
