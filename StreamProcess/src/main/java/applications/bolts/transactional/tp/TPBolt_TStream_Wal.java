package applications.bolts.transactional.tp;

import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.FaultToleranceConstants;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class TPBolt_TStream_Wal extends TPBolt_TStream{
    public TPBolt_TStream_Wal(int fid) {
        super(fid);
        this.configPrefix="tptxn";
        status=new Status();
        this.setStateful();
    }


    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if (in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                this.collector.ack(in,in.getMarker());
                switch (in.getMarker().getValue()){
                    case "recovery":
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        this.registerRecovery();
                        break;
                    case "marker":
                        TXN_PROCESS_FT();
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                    case "finish":
                        if(LREvents.size()!=0){
                            TXN_PROCESS_FT();
                        }
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                }
            }
        }else{
            execute_ts_normal(in);
        }
    }

    @Override
    protected void TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        int FT=transactionManager.start_evaluate(thread_Id,this.fid);
        switch (FT){
            case 0:
                this.AsyncRegisterPersist();
                REQUEST_REQUEST_CORE();
                REQUEST_POST();
                this.SyncCommitLog();
                LREvents.clear();//clear stored events.
                BUFFER_PROCESS();
                bufferedTuple.clear();
                break;
            case 1:
                this.SyncRegisterUndo();
                this.AsyncReConstructRequest();
                this.TXN_PROCESS_FT();
                break;
            case 2:
                this.SyncRegisterRecovery();
                this.AsyncReConstructRequest();
                this.TXN_PROCESS_FT();
                break;
        }
    }

    @Override
    protected void TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {

    }
}
