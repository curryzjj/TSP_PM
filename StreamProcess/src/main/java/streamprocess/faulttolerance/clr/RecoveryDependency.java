package streamprocess.faulttolerance.clr;

import streamprocess.components.grouping.Grouping;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.output.Epoch.EpochInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RecoveryDependency {
    //<PartitionId,List<Dependency Partition>>
    public ConcurrentHashMap<Integer, List<Integer>> recoveryDependency;
    public long currentMarkId;
    public RecoveryDependency(int partitionNum,long currentMarkId) {
        this.recoveryDependency = new ConcurrentHashMap<>();
        this.currentMarkId = currentMarkId;
        for (int i = 0; i < partitionNum; i++) {
            this.recoveryDependency.put(i,new ArrayList<>());
            this.recoveryDependency.get(i).add(i);
        }
    }
    public RecoveryDependency(ConcurrentHashMap<Integer, List<Integer>> lastDependency, long currentMarkId) {
        this.recoveryDependency =new ConcurrentHashMap<>();
        for (int id : lastDependency.keySet()) {
            List<Integer> dependencyId = new ArrayList<>(lastDependency.get(id));
            this.recoveryDependency.put(id,dependencyId);
        }
        this.currentMarkId = currentMarkId;
    }
    public ConcurrentHashMap<Integer, List<Integer>> getRecoveryDependency(){
        return this.recoveryDependency;
    }
    public void addDependency(EpochInfo epochInfo) {
        for (Map.Entry<Integer,List<Integer>> dependency : epochInfo.getModifyDependency().entrySet()) {
            for (int dependencyId : dependency.getValue()) {
                if (!this.recoveryDependency.get(dependency.getKey()).contains(dependencyId)){
                    this.recoveryDependency.get(dependency.getKey()).add(dependencyId);
                }
                if (!this.recoveryDependency.get(dependencyId).contains(dependency.getKey())){
                    this.recoveryDependency.get(dependencyId).add(dependency.getKey());
                }
            }
        }
        for (Map.Entry<Integer,List<Integer>> dependency : epochInfo.getReadOnlyDependency().entrySet()) {
            for (int dependencyId : dependency.getValue()) {
                if (!this.recoveryDependency.get(dependency.getKey()).contains(dependencyId)){
                    this.recoveryDependency.get(dependency.getKey()).add(dependencyId);
                }
            }
        }
    }
    public List<Integer> getDependencyByPartitionId(List<Integer> targetIds){
        List<Integer> dependencyIds = new ArrayList<>();
        for (int id : targetIds) {
            dependencyIds.addAll(this.recoveryDependency.get(id));
        }
        return dependencyIds;
    }
}
