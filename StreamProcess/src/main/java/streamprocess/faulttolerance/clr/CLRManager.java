package streamprocess.faulttolerance.clr;

import streamprocess.faulttolerance.FTManager;
import streamprocess.faulttolerance.FaultToleranceConstants;

import java.io.IOException;

public class CLRManager extends FTManager {
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
