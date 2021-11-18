package streamprocess.faulttolerance.checkpoint;

import engine.shapshot.SnapshotResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.ExecutionNode;
import streamprocess.faulttolerance.logger.LoggerManager;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointManager {
    private final Logger LOG= LoggerFactory.getLogger(CheckpointManager.class);
    public Status status=null;
    private LoggerManager LM;
    private String Current_Path;
    private ConcurrentHashMap<Long,Boolean> isCommitted;
    public CheckpointManager(){
        this.isCommitted=new ConcurrentHashMap<>();
    }
    public void commitCurrentLog(SnapshotResult snapshotResult){
        System.out.println("commit the checkpoint");
    }
    public boolean registerSnapshot(long checkpointId){
        if(isCommitted.containsValue(false)){
            return false;
        }else {
            isCommitted.put(checkpointId,false);
            System.out.println("register the checkpoint");
            return true;
        }
    }
}
