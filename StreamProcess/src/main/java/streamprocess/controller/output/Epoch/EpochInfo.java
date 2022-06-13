package streamprocess.controller.output.Epoch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EpochInfo implements Serializable {
    private static final long serialVersionUID = -5822653031702569315L;
    private long offset;
    private boolean isCommit;
    private int executorId;
    //<PartitionID, DependencyPartitionID>
    private final HashMap<Integer, List<Integer>> ModifyDependency;
    private final HashMap<Integer, List<Integer>> ReadOnlyDependency;
    public EpochInfo(long offset, int executorId){
        this.isCommit = false;
        this.executorId = executorId;
        this.offset = offset;
        this.ModifyDependency = new HashMap<>();
        this.ReadOnlyDependency = new HashMap<>();
    }
    public void addDependency(int[] partitionId, boolean isWrite){
        int currentId = partitionId[0];
        if (isWrite) {
            this.ModifyDependency.putIfAbsent(currentId, new ArrayList<>());
            for ( int j : partitionId) {
                if (!this.ModifyDependency.get(currentId).contains(j)){
                    this.ModifyDependency.get(currentId).add(j);
                }
            }
        } else {
            this.ReadOnlyDependency.putIfAbsent(currentId, new ArrayList<>());
            for ( int j : partitionId) {
                if (!this.ReadOnlyDependency.get(currentId).contains(j)){
                    this.ReadOnlyDependency.get(currentId).add(j);
                }
            }
        }
    }
    public void Init(long offset) {
        this.isCommit = false;
        this.offset = offset;
        ModifyDependency.clear();
        ReadOnlyDependency.clear();
    }

    public HashMap<Integer, List<Integer>> getReadOnlyDependency() {
        return ReadOnlyDependency;
    }

    public HashMap<Integer, List<Integer>> getModifyDependency() {
        return ModifyDependency;
    }
}
