package streamprocess.faulttolerance;

import streamprocess.faulttolerance.clr.ComputationLogic;
import streamprocess.faulttolerance.clr.ComputationTask;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

public abstract class FTManager extends Thread {
    public boolean running=true;
    public abstract void initialize(boolean needRecovery) throws IOException;

    /**
     * To register the wal(globalLSN) or the snapshot(checkpointId), or recovery
     * @param id
     * @return
     */
    public boolean spoutRegister(long id){
        return true;
    }

    /**
     * To register the commit after finish the transaction group or to notify the recoveryManager to start recovery
     * @param executorId
     */
    public abstract void boltRegister(int executorId, FaultToleranceConstants.FaultToleranceStatus status);
    public abstract Object getLock();
    public abstract void close();
    public void commitComputationTasks(List<ComputationTask> tasks){
    }
    public void commitComputationLogics(List<ComputationLogic> logics){

    }
    public Queue getComputationTasks(int executorId){
        return null;
    }
}
