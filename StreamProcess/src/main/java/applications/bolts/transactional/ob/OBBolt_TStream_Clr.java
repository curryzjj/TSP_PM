package applications.bolts.transactional.ob;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import streamprocess.controller.output.Epoch.EpochInfo;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.*;

public class OBBolt_TStream_Clr extends OBBolt_TStream{
    private static final long serialVersionUID = 2185341632820954575L;

    public OBBolt_TStream_Clr(int fid) {
        super(fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(in.isMarker()){
            //this.collector.ack(in,in.getMarker());
            if (status.allMarkerArrived(in.getSourceTask(),this.executor)){
                switch (in.getMarker().getValue()){
                    case "recovery":
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                    case "marker":
                        this.markerId = in.getBID();
                        if (enable_determinants_log && this.markerId < recoveryId) {
                            this.CommitOutsideDeterminant(this.markerId);
                        }
                        if (TXN_PROCESS()){
                            if (enable_recovery_dependency) {
                                Marker marker = in.getMarker().clone();
                                marker.setEpochInfo(this.epochInfo);
                                forward_marker(in.getSourceTask(),in.getBID(),marker,marker.getValue());
                                this.epochInfo = new EpochInfo(in.getBID(), executor.getExecutorID());
                            } else {
                                forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                            }
                        }
                        break;
                    case "snapshot":
                        this.markerId = in.getBID();
                        this.isSnapshot = true;
                        if (TXN_PROCESS_FT()){
                            Marker marker = in.getMarker();
                            marker.setEpochInfo(this.epochInfo);
                            forward_marker(in.getSourceTask(),in.getBID(),marker,marker.getValue());
                        }
                        break;
                    case "finish":
                        this.markerId = in.getBID();
                        if (enable_determinants_log && this.markerId < recoveryId) {
                            this.CommitOutsideDeterminant(this.markerId);
                        }
                        if(TXN_PROCESS()){
                            /* All the data has been executed */
                            if (enable_recovery_dependency) {
                                Marker marker = in.getMarker().clone();
                                marker.setEpochInfo(this.epochInfo);
                                forward_marker(in.getSourceTask(),in.getBID(),marker,marker.getValue());
                                this.epochInfo = new EpochInfo(in.getBID(), executor.getExecutorID());
                            } else {
                                forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                            }
                        }
                        this.context.stop_running();
                        break;
                }
            }
        }else {
            execute_ts_normal(in);
        }
    }

    @Override
    protected boolean TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        int FT=transactionManager.start_evaluate(thread_Id,this.markerId);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        boolean transactionSuccess=FT==0;
        switch (FT){
            case 0:
                this.AsyncRegisterPersist();
                MeasureTools.startPost(this.thread_Id,System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPost(this.thread_Id,System.nanoTime());
                this.SyncCommitLog();
                EventsHolder.clear();//clear stored events.
                BUFFER_PROCESS();
                break;
            case 1:
                this.SyncRegisterUndo();
                this.AsyncReConstructRequest();
                transactionSuccess=this.TXN_PROCESS_FT();
                break;
            case 2:
                if (enable_align_wait){
                    this.collector.cleanAll();
                } else {
                    for (int partitionId:this.db.getTxnProcessingEngine().getRecoveryRangeId()) {
                        if(executor.getExecutorID() == executor.operator.getExecutorIDList().get(partitionId)) {
                            this.collector.cleanAll();
                            break;
                        }
                    }
                }
                this.SyncRegisterRecovery();
                this.EventsHolder.clear();
                for (Queue<Tuple> tuples : bufferedTuples.values()) {
                    tuples.clear();
                }
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
                MeasureTools.startPost(this.thread_Id,System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPost(this.thread_Id,System.nanoTime());
                EventsHolder.clear();//clear stored events.
                BUFFER_PROCESS();
                break;
            case 1:
                this.SyncRegisterUndo();
                this.AsyncReConstructRequest();
                transactionSuccess=this.TXN_PROCESS_FT();
                break;
            case 2:
                if (enable_align_wait){
                    this.collector.cleanAll();
                } else {
                    for (int partitionId:this.db.getTxnProcessingEngine().getRecoveryRangeId()) {
                        if(executor.getExecutorID() == executor.operator.getExecutorIDList().get(partitionId)) {
                            this.collector.cleanAll();
                            break;
                        }
                    }
                }
                this.SyncRegisterRecovery();
                this.EventsHolder.clear();
                for (Queue<Tuple> tuples : bufferedTuples.values()) {
                    tuples.clear();
                }
                break;
        }
        return transactionSuccess;
    }
}
