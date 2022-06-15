package applications.bolts.transactional.gs;

import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.faulttolerance.checkpoint.Status;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class GSBolt_TStream_NoFT extends GSBolt_TStream{
    private static final long serialVersionUID = 5698215001916299700L;

    public GSBolt_TStream_NoFT(int fid) {
        super(fid);
    }
    private int flag = 0;
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(in.isMarker()){
            if (status.allMarkerArrived(in.getSourceTask(),this.executor)){
                switch (in.getMarker().getValue()) {
                    case "marker":
                        this.markerId = in.getBID();
                        TXN_PROCESS();
                       // forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                        break;
                    case "finish":
                        this.markerId = in.getBID();
                        if(TXN_PROCESS()){
                            /* All the data has been executed */
                            forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
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
        return false;
    }

    @Override
    protected boolean TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        transactionManager.start_evaluate(thread_Id, this.markerId);
        REQUEST_CORE();
        REQUEST_POST();
        EventsHolder.clear();//clear stored events.
        BUFFER_PROCESS();
        return true;
    }
}
