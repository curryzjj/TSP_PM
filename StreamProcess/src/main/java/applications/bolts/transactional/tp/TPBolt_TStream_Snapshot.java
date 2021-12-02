package applications.bolts.transactional.tp;

import engine.Exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class TPBolt_TStream_Snapshot extends TPBolt_TStream {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_TStream_Snapshot.class);
    public TPBolt_TStream_Snapshot(int fid) {
        super(fid);
        this.configPrefix="tptxn";
        status=new Status();
        this.setStateful();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(in.isMarker()){
            if(status.allMarkerArrived(in.getSourceTask(),this.executor)){
                this.collector.ack(in,in.getMarker());
                switch (in.getMarker().getValue()){
                    case "recovery":
                        forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                    case "marker":
                        if(TXN_PROCESS()){
                            /* When the transaction is successful, the data can be pre-commit to the outside world */
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        }
                        break;
                    case "snapshot":
                        this.needcheckpoint=true;
                        this.checkpointId=in.getBID();
                        if(TXN_PROCESS_FT()){
                            /* When the snapshot is completed, the data can be consumed by the outside world */
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        }
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
    protected boolean TXN_PROCESS_FT() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
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
            case 2:
                this.SyncRegisterRecovery();
                this.collector.clean();
                this.LREvents.clear();
                this.bufferedTuple.clear();
                break;
        }
        return FT==0;
    }

    @Override
    protected boolean TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        int FT=transactionManager.start_evaluate(thread_Id,this.fid);
        switch (FT){
            case 0:
                REQUEST_REQUEST_CORE();
                REQUEST_POST();
                LREvents.clear();
                BUFFER_PROCESS();
                bufferedTuple.clear();
                break;
            case 1:
            case 2:
                this.SyncRegisterRecovery();
                this.collector.clean();
                this.LREvents.clear();
                this.bufferedTuple.clear();
                break;
        }
        return FT==0;
    }
}
