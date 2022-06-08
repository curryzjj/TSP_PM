package engine.shapshot;


import System.FileSystem.Path;
import engine.table.keyGroup.KeyGroupRangeOffsets;
import scala.Tuple2;

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
    private HashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> snapshotResults;
    private int taskId;

    private Path snapshotPath;
    private KeyGroupRangeOffsets keyGroupRangeOffsets;
    private long checkpointId;
    private long timestamp;
    public SnapshotResult(Path snapshotPath, KeyGroupRangeOffsets keyGroupRangeOffsets,long timestamp,long checkpointId){
        this.snapshotPath = snapshotPath;
        this.keyGroupRangeOffsets = keyGroupRangeOffsets;
        this.checkpointId = checkpointId;
        this.timestamp = timestamp;
        this.snapshotResults=new HashMap<>();
    }
    public SnapshotResult(HashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> snapshotResults,long timestamp,long checkpointId){
        this.checkpointId = checkpointId;
        this.timestamp = timestamp;
        this.snapshotResults = snapshotResults;
    }

    /**
     * Used in the recovery
     * @param path
     * @param keyGroupRangeOffsets
     */
    public SnapshotResult(Path path,KeyGroupRangeOffsets keyGroupRangeOffsets){
        this.snapshotPath = path;
        this.keyGroupRangeOffsets = keyGroupRangeOffsets;
    }
    public long getCheckpointId() {
        return checkpointId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public HashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> getSnapshotResults() {
        if (snapshotResults.size() == 0){
            HashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> snapshotResult=new HashMap<>();
            Tuple2 tuple = new Tuple2(this.snapshotPath,this.keyGroupRangeOffsets);
            snapshotResult.put(0,tuple);
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
