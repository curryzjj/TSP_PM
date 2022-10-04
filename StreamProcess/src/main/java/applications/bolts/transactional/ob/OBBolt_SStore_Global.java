package applications.bolts.transactional.ob;

import UserApplications.CONTROL;
import applications.events.GlobalSorter;
import applications.events.TxnEvent;
import engine.Exception.DatabaseException;
import streamprocess.execution.runtime.tuple.Tuple;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static UserApplications.CONTROL.lostPartitionId;

public class OBBolt_SStore_Global extends OBBolt_SStore{
    private static final long serialVersionUID = 7285315358359232553L;
    public OBBolt_SStore_Global(int fid) {
        super(fid);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException, ExecutionException {
        if(CONTROL.failureFlag.get()){
            if (this.executor.isFirst_executor()) {
                this.db.getTxnProcessingEngine().mimicFailure(lostPartitionId);
                CONTROL.failureFlagBid.add(in.getBID());
                GlobalSorter.sortedEvents.clear();
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
                    if(status.allMarkerArrived(in.getSourceTask(), this.executor)){
                        switch (in.getMarker().getValue()){
                            case "marker":
                                this.markerId = in.getBID();
                                if (TXN_PROCESS()) {
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                }
                                break;
                            case "snapshot":
                                this.checkpointId = this.markerId = in.getBID();
                                if (TXN_PROCESS_FT()) {
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                }
                                break;
                            case "finish":
                                this.markerId = in.getBID();
                                if (TXN_PROCESS()) {
                                    forward_marker(in.getSourceTask(),in.getBID(),in.getMarker(),in.getMarker().getValue());
                                    this.context.stop_running();
                                }
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
        if (Sort_Lock(this.thread_Id)) {
            for (int i = 0; i < this.EventsHolder.size(); i ++) {
                TxnEvent event = this.EventsHolder.get(i);
                LAL_PROCESS(event);
                PostLAL_Process(event, i == this.EventsHolder.size() - 1);
                POST_PROCESS(event);
            }
            this.EventsHolder.clear();
            BUFFER_PROCESS();
            return true;
        } else {
            if (this.executor.isFirst_executor()) {
                this.db.getTxnProcessingEngine().mimicFailure(lostPartitionId);
                CONTROL.failureFlagBid.add(markerId);
                GlobalSorter.sortedEvents.clear();
            }
            this.SyncRegisterRecovery();
            this.collector.cleanAll();
            this.EventsHolder.clear();
            for (Queue<Tuple> tuples : bufferedTuples.values()) {
                tuples.clear();
            }
            return false;
        }
    }

    @Override
    protected boolean TXN_PROCESS() throws DatabaseException, InterruptedException, BrokenBarrierException, IOException, ExecutionException {
        if (Sort_Lock(this.thread_Id)) {
            for (TxnEvent event : this.EventsHolder) {
                LAL_PROCESS(event);
                PostLAL_Process(event, false);
                POST_PROCESS(event);
            }
            this.EventsHolder.clear();
            BUFFER_PROCESS();
            return true;
        } else {
            if (this.executor.isFirst_executor()) {
                this.db.getTxnProcessingEngine().mimicFailure(lostPartitionId);
                CONTROL.failureFlagBid.add(markerId);
                GlobalSorter.sortedEvents.clear();
            }
            this.SyncRegisterRecovery();
            this.collector.cleanAll();
            this.EventsHolder.clear();
            for (Queue<Tuple> tuples : bufferedTuples.values()) {
                tuples.clear();
            }
            return false;
        }
    }
}
