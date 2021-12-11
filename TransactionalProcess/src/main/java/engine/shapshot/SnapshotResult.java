package engine.shapshot;


import System.FileSystem.Path;
import engine.table.keyGroup.KeyGroupRangeOffsets;

import java.io.Serializable;
import java.util.HashMap;

public class SnapshotResult implements Serializable {
    private static final long serialVersionUID = 8003076517462798444L;
    /** An singleton instance to represent an empty snapshot result. */
    private static SnapshotResult EMPTY = new SnapshotResult();
    public SnapshotResult() {
    }
    public static <T> SnapshotResult empty() {
        return (SnapshotResult) EMPTY;
    }
    /** Use in the parallel snapshot */
    private HashMap<Path,KeyGroupRangeOffsets> snapshotResults;
    private int taskId;

    private Path snapshotPath;
    private KeyGroupRangeOffsets keyGroupRangeOffsets;
    private long checkpointId;
    private long timestamp;
    public SnapshotResult(Path snapshotPath, KeyGroupRangeOffsets keyGroupRangeOffsets,long timestamp,long checkpointId){
        this.snapshotPath = snapshotPath;
        this.keyGroupRangeOffsets = keyGroupRangeOffsets;
        this.checkpointId=checkpointId;
        this.timestamp=timestamp;
        this.snapshotResults=new HashMap<>();
    }
    public SnapshotResult(HashMap<Path,KeyGroupRangeOffsets> snapshotResults,long timestamp,long checkpointId){
        this.checkpointId=checkpointId;
        this.timestamp=timestamp;
        this.snapshotResults=snapshotResults;
    }

    /**
     * Used in the recovery
     * @param path
     * @param keyGroupRangeOffsets
     */
    public SnapshotResult(Path path,KeyGroupRangeOffsets keyGroupRangeOffsets){
        this.snapshotPath=path;
        this.keyGroupRangeOffsets=keyGroupRangeOffsets;
    }
    public long getCheckpointId() {
        return checkpointId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public HashMap<Path, KeyGroupRangeOffsets> getSnapshotResults() {
        if (snapshotResults.size()==0){
            HashMap<Path,KeyGroupRangeOffsets> snapshotResult=new HashMap<>();
            snapshotResult.put(this.snapshotPath,this.keyGroupRangeOffsets);
            return snapshotResult;
        }else{
            return snapshotResults;
        }
    }

    public Path getSnapshotPath() {
        return snapshotPath;
    }

    public KeyGroupRangeOffsets getKeyGroupRangeOffsets() {
        return keyGroupRangeOffsets;
    }
}
