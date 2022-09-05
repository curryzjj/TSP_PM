package applications.bolts.transactional.sl;

import System.measure.MeasureTools;
import UserApplications.CONTROL;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;
import static UserApplications.CONTROL.enable_upstreamBackup;

public class SLBolt_TStream_Local extends SLBolt_TStream_Conventional{
    private static final long serialVersionUID = 5796686220206329210L;

    public SLBolt_TStream_Local(int fid) {super(fid);}
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if (failureFlag.get()) {
            if (this.executor.isFirst_executor()) {
                this.db.getTxnProcessingEngine().mimicFailure(lostPartitionId);
                CONTROL.failureFlagBid.add(in.getBID());
            }
            this.recoveryPartitionIds.add(lostPartitionId);
            this.SyncRegisterRecovery();
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
            if (enable_upstreamBackup) {
                this.multiStreamInFlightLog.cleanAll(DEFAULT_STREAM_ID);
            }
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
                        switch (in.getMarker().getValue()){
                            case "recovery":
                                forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                break;
                            case "marker":
                                this.markerId = in.getBID();
                                if (TXN_PROCESS()){
                                    if (this.markerId > recoveryId) {
                                        Marker marker = in.getMarker().clone();
                                        forward_marker(in.getSourceTask(),in.getBID(),marker,marker.getValue());
                                        if (enable_upstreamBackup) {
                                            this.multiStreamInFlightLog.addBatch(this.markerId, DEFAULT_STREAM_ID);
                                        }
                                        MeasureTools.HelpLog_finish_acc(this.thread_Id);
                                        MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                    }
                                }
                                break;
                            case "snapshot":
                                this.markerId = in.getBID();
                                this.isSnapshot = true;
                                if (TXN_PROCESS_FT()) {
                                    Marker marker = in.getMarker();
                                    forward_marker(in.getSourceTask(), in.getBID(), marker, marker.getValue());
                                    if (enable_upstreamBackup) {
                                        this.multiStreamInFlightLog.addEpoch(this.markerId, DEFAULT_STREAM_ID);
                                        this.multiStreamInFlightLog.addBatch(this.markerId, DEFAULT_STREAM_ID);
                                    }
                                    MeasureTools.HelpLog_finish_acc(this.thread_Id);
                                    MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                }
                                break;
                            case "finish":
                                this.markerId = in.getBID();
                                if(TXN_PROCESS()){
                                    /* All the data has been executed */
                                    if (this.markerId > recoveryId) {
                                        Marker marker = in.getMarker().clone();
                                        forward_marker(in.getSourceTask(),in.getBID(),marker,marker.getValue());
                                        if (enable_upstreamBackup) {
                                            this.multiStreamInFlightLog.addBatch(this.markerId, DEFAULT_STREAM_ID);
                                        }
                                        MeasureTools.HelpLog_finish_acc(this.thread_Id);
                                        MeasureTools.Transaction_construction_finish_acc(this.thread_Id);
                                    }
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
        boolean transactionSuccess = FT==0;
        this.AsyncRegisterPersist();
        switch (FT){
            case 0:
                MeasureTools.startPostTransaction(this.thread_Id, System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPostTransaction(this.thread_Id, System.nanoTime());
                EventsHolder.clear();//clear stored events.
                BUFFER_PROCESS();
                break;
            case 1:
                MeasureTools.Transaction_abort_begin(this.thread_Id, System.nanoTime());
                transactionSuccess = this.TXN_PROCESS();
                MeasureTools.Transaction_abort_finish(this.thread_Id, System.nanoTime());
                break;
            case 2:
                if (this.executor.isFirst_executor()) {
                    this.db.getTxnProcessingEngine().mimicFailure(lostPartitionId);
                    CONTROL.failureFlagBid.add(markerId);
                }
                this.recoveryPartitionIds.add(lostPartitionId);
                this.SyncRegisterRecovery();
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
                if (enable_upstreamBackup) {
                    this.multiStreamInFlightLog.cleanAll(DEFAULT_STREAM_ID);
                }
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
        int FT = transactionManager.start_evaluate(thread_Id, this.markerId);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        boolean transactionSuccess = FT == 0;
        switch (FT){
            case 0:
                MeasureTools.startPostTransaction(this.thread_Id, System.nanoTime());
                REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPostTransaction(this.thread_Id, System.nanoTime());
                EventsHolder.clear();//clear stored events.
                BUFFER_PROCESS();
                break;
            case 1:
                MeasureTools.Transaction_abort_begin(this.thread_Id, System.nanoTime());
                transactionSuccess = this.TXN_PROCESS();
                MeasureTools.Transaction_abort_finish(this.thread_Id, System.nanoTime());
                break;
            case 2:
                if (this.executor.isFirst_executor()) {
                    this.db.getTxnProcessingEngine().mimicFailure(lostPartitionId);
                    CONTROL.failureFlagBid.add(markerId);
                }
                this.recoveryPartitionIds.add(lostPartitionId);
                this.SyncRegisterRecovery();
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
                if (enable_upstreamBackup) {
                    this.multiStreamInFlightLog.cleanAll(DEFAULT_STREAM_ID);
                }
                this.EventsHolder.clear();
                for (Queue<Tuple> tuples : bufferedTuples.values()) {
                    tuples.clear();
                }
                break;
        }
        return transactionSuccess;
    }
}
