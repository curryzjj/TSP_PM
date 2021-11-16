package streamprocess.faulttolerance.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.logger.LoggerManager;

import java.io.File;

public class CheckpointManager {
    private final Logger LOG= LoggerFactory.getLogger(CheckpointManager.class);
    public Status status=null;
    private LoggerManager LM;
    private String Current_Path;
    public void commitCurrentLog(){
        File file=new File(Current_Path);
    }
}
