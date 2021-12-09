package streamprocess.faulttolerance.clr;

import System.FileSystem.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.FTManager;
import streamprocess.faulttolerance.FaultToleranceConstants;

import java.io.IOException;

public class CLRManager extends FTManager {
    private final Logger LOG= LoggerFactory.getLogger(CLRManager.class);
    public boolean running=true;
    private FileSystem localFS;
    @Override
    public void initialize(boolean needRecovery) throws IOException {

    }

    @Override
    public void boltRegister(int executorId, FaultToleranceConstants.FaultToleranceStatus status) {

    }

    @Override
    public Object getLock() {
        return null;
    }

    @Override
    public void close() {

    }
}
