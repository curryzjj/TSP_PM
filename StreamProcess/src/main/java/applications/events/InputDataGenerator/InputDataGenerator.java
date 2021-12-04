package applications.events.InputDataGenerator;

import System.tools.FastZipfGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.TxnEvent;
import applications.events.TxnParam;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import static UserApplications.CONTROL.enable_states_partition;

public abstract class InputDataGenerator implements Serializable {
    /**
     * Generate data in batch and store in the input store
     * @param batch
     * @return
     */
    //different R-W ratio.
    //just enable one of the decision array
    protected transient boolean[] read_decision;
    protected long[] p_bid;//used for partition.
    protected int current_pid;
    protected int read_decision_id;
    protected String dataPath;
    protected int recordNum;
    protected double zipSkew;
    protected int range;
    protected int access_per_partition;
    protected int floor_interval;
    protected int tthread;
    protected final String split_exp = ";";
    public static FastZipfGenerator shared_store;
    public static FastZipfGenerator[] partitioned_store;
    public abstract List<AbstractInputTuple> generateData(int batch);
    public abstract List<TxnEvent> generateEvent(int batch);
    public abstract void initialize(String dataPath, int recordNum, int range, double zipSkew, Configuration config);
    public abstract Object create_new_event(int bid);
    public abstract void close();
    protected void randomKeys(int pid, TxnParam param, Set keys,int access_per_partition,int counter,int numAccessesPerEvent){
        for (int access_id=0;access_id<numAccessesPerEvent;++access_id){
            FastZipfGenerator generator;
            if(enable_states_partition){
                generator= partitioned_store[pid];
            }else {
                generator=shared_store;
            }
            int res=generator.next();
            while(keys.contains(res)&&!Thread.currentThread().isInterrupted()){
                res=generator.next();
            }
            keys.add(res);
            param.set_keys(access_id,res);
            counter++;
            if (counter==access_per_partition){
                pid++;
                if(pid==tthread){
                    pid=0;
                }
                counter++;
            }
        }
    }
    protected boolean next_read_decision() {
        boolean rt = read_decision[read_decision_id];
        read_decision_id++;
        if (read_decision_id == 8)
            read_decision_id = 0;
        return rt;
    }
    protected  boolean verify(Set keys, int partition_id, int number_of_partitions) {

        for (Object key : keys) {
            int i = (Integer) key;
            int pid = i / (floor_interval);

            boolean case1 = pid >= partition_id && pid <= partition_id + number_of_partitions;
            boolean case2 = pid >= 0 && pid <= (partition_id + number_of_partitions) % tthread;

            if (!(case1 || case2)) {
                return false;
            }
        }

        return true;
    }
}
