package streamprocess.faulttolerance.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.logger.LoggerManager;

public class CheckpointManager {
    private final Logger LOG= LoggerFactory.getLogger(CheckpointManager.class);
    public Status status=null;
    private LoggerManager LM;

}
