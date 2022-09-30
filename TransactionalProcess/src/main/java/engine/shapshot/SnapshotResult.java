package engine.shapshot;


import System.FileSystem.Path;
import engine.table.keyGroup.KeyGroupRangeOffsets;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SnapshotResult implements Serializable {
    private static final long serialVersionUID = 8003076517462798444L;
    /** An singleton instance to represent an empty snapshot result. */
    private static SnapshotResult EMPTY = new SnapshotResult();
    public SnapshotResult() {
    }
    public SnapshotResult(long checkpointId) {
        this.checkpointId = checkpointId;
        this.snapshotResults = new ConcurrentHashMap<>();
    }
    public static <T> SnapshotResult empty() {
        return (SnapshotResult) EMPTY;
    }
    /** Use in the parallel snapshot */
    //<partitionId, Tuple2>
    private ConcurrentHashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> snapshotResults;
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
        this.snapshotResults = new ConcurrentHashMap<>();
    }
    public SnapshotResult(ConcurrentHashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> snapshotResults,long timestamp,long checkpointId){
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

    public ConcurrentHashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> getSnapshotResults() {
        if (snapshotResults.size() == 0){
            ConcurrentHashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> snapshotResult = new ConcurrentHashMap<>();
            Tuple2 tuple = new Tuple2(this.snapshotPath,this.keyGroupRangeOffsets);
            snapshotResult.put(0,tuple);
            return snapshotResult;
        }else{
            return snapshotResults;
        }
    }

    public void setSnapshotResults(int partitionId, Tuple2<Path,KeyGroupRangeOffsets> results){
        this.snapshotResults.put(partitionId,results);
    }

    public Path getSnapshotPath() {
        return snapshotPath;
    }
    public List<Path> getSnapshotPaths(){
        List<Path> paths = new ArrayList<>();
        for (Tuple2<Path,KeyGroupRangeOffsets> tuple2 : snapshotResults.values()) {
            paths.add(tuple2._1);
        }
        return paths;
    }

    public KeyGroupRangeOffsets getKeyGroupRangeOffsets() {
        return keyGroupRangeOffsets;
    }
}
