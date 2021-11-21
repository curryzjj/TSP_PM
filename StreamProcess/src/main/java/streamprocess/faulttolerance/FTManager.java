package streamprocess.faulttolerance;

import java.io.IOException;

public abstract class FTManager extends Thread {
    public boolean running=true;
    public abstract void initialize() throws IOException;

    /**
     * To register the wal(globalLSN) or the snapshot(checkpointId)
     * @param id
     * @return
     */
    public abstract boolean spoutRegister(long id);

    /**
     * To register the commit after finish the transaction group
     * @param executorId
     */
    public abstract void boltRegister(int executorId);
    public abstract Object getLock();
    public abstract void close();
}
