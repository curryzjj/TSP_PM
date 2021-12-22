package streamprocess.faulttolerance.clr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static UserApplications.CONTROL.partition_num;

/**
 * ComputationLogic record the operation logic to partitioned states
 * When recovery the computationLogic can be transfer to computationTask
 */
public class ComputationLogic implements Serializable {
    private static final long serialVersionUID = -2276549562019953383L;
    public long bid;
    public HashMap<Integer, List<Integer>> indexByPartition;
    public ComputationLogic(long bid){
        this.bid=bid;
        this.indexByPartition=new HashMap<>();
        for (int i=0;i<partition_num;i++){
            this.indexByPartition.put(i,new ArrayList<>());
        }
    }
    public String toString(int partitionId){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append(this.bid);
        stringBuilder.append(" ");
        stringBuilder.append(getIndexString(partitionId));
        return stringBuilder.toString();
    }
    private String getIndexString(int partitionId){
        StringBuilder stringBuilder=new StringBuilder();
        for (int str:indexByPartition.get(partitionId)){
            stringBuilder.append(str+" ");
        }
        return stringBuilder.toString();
    }
    public void putIndex(int partitionId,int index){
        this.indexByPartition.get(partitionId).add(index);
    }

}
