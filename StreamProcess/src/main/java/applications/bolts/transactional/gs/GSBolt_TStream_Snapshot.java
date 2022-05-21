package applications.bolts.transactional.gs;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class GSBolt_TStream_Snapshot extends GSBolt_TStream{
    public GSBolt_TStream_Snapshot(int fid) {
        super(fid);
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                //this.collector.ack(in,in.getMarker());
                switch (in.getMarker().getValue()){
                    case "recovery":
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                    case "marker":
                        TXN_PROCESS();
                        break;
                    case "finish":
                        if(TXN_PROCESS()){
                            /* All the data has been executed */
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        }
                        this.context.stop_running();
                        break;
                    case "snapshot":
                        this.needcheckpoint=true;
                        this.checkpointId=in.getBID();
                        if(TXN_PROCESS_FT()){
                            /* When the snapshot is completed, the data can be consumed by the outside world */
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        }
                        break;
                }
            }
        }else{
            execute_ts_normal(in);
        }
    }

    @Override
    protected boolean TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        int FT=transactionManager.start_evaluate(thread_Id,this.fid);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        switch (FT){
            case 0:
                this.AsyncRegisterPersist();
                MeasureTools.startPost(this.thread_Id,System.nanoTime());
                REQUEST_REQUEST_CORE();
                REQUEST_POST();
                MeasureTools.finishPost(this.thread_Id,System.nanoTime());
                this.SyncCommitLog();
                EventsHolder.clear();//clear stored events.
                BUFFER_PROCESS();
                bufferedTuple.clear();
                break;
            case 1:
            case 2:
                this.SyncRegisterRecovery();
                this.collector.cleanAll();
                this.EventsHolder.clear();
                this.bufferedTuple.clear();
                break;
        }
        return FT==0;
    }

    @Override
    protected boolean TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        MeasureTools.startTransaction(this.thread_Id,System.nanoTime());
        int FT=transactionManager.start_evaluate(thread_Id,this.fid);
        MeasureTools.finishTransaction(this.thread_Id,System.nanoTime());
        switch (FT){
            case 0:
                MeasureTools.startPost(this.thread_Id,System.nanoTime());
                REQUEST_REQUEST_CORE();
                /* When the transaction is successful, the data can be pre-commit to the outside world */
                REQUEST_POST();
                MeasureTools.finishPost(this.thread_Id,System.nanoTime());
                EventsHolder.clear();
                BUFFER_PROCESS();
                bufferedTuple.clear();
                break;
            case 1:
            case 2:
                this.SyncRegisterRecovery();
                this.collector.cleanAll();
                this.EventsHolder.clear();
                this.bufferedTuple.clear();
                break;
        }
        return FT==0;
    }
}
