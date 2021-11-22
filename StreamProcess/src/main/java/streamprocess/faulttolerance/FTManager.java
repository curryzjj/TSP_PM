package streamprocess.faulttolerance;

import java.io.IOException;

public abstract class FTManager extends Thread {
    public boolean running=true;
    public abstract void initialize() throws IOException;

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
    public abstract void boltRegister(int executorId);
    public abstract Object getLock();
    public abstract void close();
}
