package applications.bolts.transactional.gs;

import System.measure.MeasureTools;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class GSBolt_TStream_Snapshot extends GSBolt_TStream{
    private static final long serialVersionUID = 6126484047591969728L;
    public GSBolt_TStream_Snapshot(int fid) {
        super(fid);
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                switch (in.getMarker().getValue()){
                    case "recovery":
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                    case "marker":
                        this.markerId = in.getBID();
                        TXN_PROCESS();
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                    case "snapshot":
                        this.markerId = in.getBID();
                        this.isSnapshot = true;
                        if(TXN_PROCESS_FT()){
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        }
                        break;
                    case "finish":
                        this.markerId = in.getBID();
                        if(TXN_PROCESS()){
                            /* All the data has been executed */
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        }
                        this.context.stop_running();
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + in.getMarker().getValue());
                }
            }
        }else{
            execute_ts_normal(in);
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
                transactionSuccess = this.TXN_PROCESS_FT();
                break;
            case 2:
                this.SyncRegisterRecovery();
                this.collector.cleanAll();
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
                EventsHolder.clear();
                BUFFER_PROCESS();
                break;
            case 1:
                this.SyncRegisterUndo();
                this.AsyncReConstructRequest();
                transactionSuccess = this.TXN_PROCESS_FT();
                break;
            case 2:
                this.SyncRegisterRecovery();
                this.collector.cleanAll();
                this.EventsHolder.clear();
                for (Queue<Tuple> tuples : bufferedTuples.values()) {
                    tuples.clear();
                }
                break;
        }
        return transactionSuccess;
    }
}
