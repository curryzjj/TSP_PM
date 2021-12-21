package streamprocess.faulttolerance.clr;
/**
 * One task access the shared state, tasks may influence each other
 * So, we combine the tasks in one EventTask
 * Task need to support Undo operation, and need to support Fine-grained recovery
 * So, we divide the EventTask into multiple TransactionTasks, and then TransactionTask are divide into OperationsTask
 * Spout->EventTask->Bolts->ComputationTask->TxnManager
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ComputationTask record the operations to partitioned states
 */
public class ComputationTask implements Serializable {
    private static final long serialVersionUID = 4822059630736962439L;
    public String event_name;
    public String key;
    public String table_name;
    public List<String> values;
    public long bid;
    public int partition_id;
    //used in the re_running the commit computationTasks
    public boolean txn_flag=false;
    public boolean finish_flag=false;
    public ComputationTask(String event_name, String key, String table_name, long bid,int partition_id,List<String> value){
        this.event_name = event_name;
        this.key = key;
        this.table_name = table_name;
        this.bid = bid;
        this.partition_id=partition_id;
        this.values=value;
    }
    public ComputationTask(long bid,String key,String table_name,String event_name,String values){
        this.event_name = event_name;
        this.key = key;
        this.table_name = table_name;
        this.bid = bid;
        String value[]=values.split(";");
        this.values=new ArrayList<>();
        for (String str:value){
            this.values.add(str);
        }
    }
    public ComputationTask(boolean txn_flag,boolean finish_flag){
        this.txn_flag=txn_flag;
        this.finish_flag=finish_flag;
    }
    public String toString(){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append(bid);
        stringBuilder.append(" ");
        stringBuilder.append(key);
        stringBuilder.append(" ");
        stringBuilder.append(table_name);
        stringBuilder.append(" ");
        stringBuilder.append(event_name);
        stringBuilder.append(" ");
        stringBuilder.append(getValueString());
        return stringBuilder.toString();
    }
    private String getValueString(){
        StringBuilder stringBuilder=new StringBuilder();
        for (String str:values){
            stringBuilder.append(str+";");
        }
        return stringBuilder.toString();
    }
    public String getValue(int index){
        return values.get(index);
    }
    public int getPartition_id() {
        return partition_id;
    }
}
