package applications.events.InputDataGenerator;

import System.tools.FastZipfGenerator;
import System.util.Configuration;
import applications.DataTypes.AbstractInputTuple;
import applications.events.TxnEvent;
import applications.events.TxnParam;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static UserApplications.CONTROL.*;
import static UserApplications.CONTROL.NUM_ITEMS;

public abstract class InputDataGenerator implements Serializable {
    private static final long serialVersionUID = 6230560242079070217L;
    /**
     * Generate data in batch and store in the input store
     * @param batch
     * @return
     */
    private final Random random = new Random(0); // the transaction type decider
    protected long[] p_bid;//used for partition.
    protected int current_bid;
    protected int current_pid;
    protected String dataPath;
    protected int recordNum;
    protected double zipSkew;
    protected int range;
    protected int access_per_partition;
    protected int floor_interval;
    protected int partition_num;
    protected int partition_num_per_txn;
    //<partitionId, dependencyId>
    protected HashMap<Integer,List<Integer>> partitionDependency;
    protected final String split_exp = ";";
    public static FastZipfGenerator shared_store;
    public static FastZipfGenerator[] partitioned_store;
    public static FastZipfGenerator partitionId_generator;
    public abstract List<AbstractInputTuple> generateData(int batch);
    public abstract List<TxnEvent> generateEvent(int batch);
    public abstract void storeInput(Object input) throws IOException;
    public abstract Object create_new_event(int bid);
    public abstract void close();
    public void initialize(String dataPath, int recordNum, int range, double zipSkew, Configuration config){
        this.recordNum = recordNum;
        this.dataPath = dataPath;
        this.zipSkew = zipSkew;
        this.range = range;
        this.current_pid = 0;
        this.partition_num = config.getInt("partition_num");
        this.partition_num_per_txn = config.getInt("partition_num_per_txn");
        this.createDependency();
        this.configure_store();
        p_bid = new long[partition_num];
        for (int i = 0; i < partition_num; i++) {
            p_bid[i] = 0;
        }
    }

    protected void randomKeys(TxnParam param, Set<Integer> keys, int numAccessesPerEvent){
        int i = 0;
        int p_id = current_pid;
        boolean isDependency = random.nextInt(1000) < RATIO_OF_DEPENDENCY;
        for (int access_id = 0; access_id<numAccessesPerEvent; ++access_id){
            FastZipfGenerator generator;
            if(enable_states_partition){
                generator = partitioned_store[p_id];
            }else {
                generator = shared_store;
            }
            int res = generator.next();
            while(keys.contains(res)&&!Thread.currentThread().isInterrupted()){
                res = generator.next();
            }
            keys.add(res);
            param.set_keys(access_id,res);
            if (isDependency) {
                i = random.nextInt(this.partitionDependency.get(current_pid).size());
                p_id = this.partitionDependency.get(current_pid).get(i);
            }
        }
    }
    protected boolean next_decision() {
        return random.nextInt(100) < RATIO_OF_READ;
    }
    protected void createDependency(){
        this.partitionDependency = new HashMap<>();
        List<Integer> partitionIds = new ArrayList<>();
        for (int i = 0; i < partition_num ; i++) {
            partitionIds.add(i);
        }
        int childNum = partitionIds.size() / partition_num_per_txn;
        int leaf = partitionIds.size() % partition_num_per_txn;
        if (leaf > 0) {
            childNum ++;
        }
        for (int i = 0; i < childNum; i ++) {
            List<Integer> list;
            if (partition_num_per_txn * (i+1) > partitionIds.size()) {
                list = partitionIds.subList( i * partition_num_per_txn, partitionIds.size());
            } else {
                list = partitionIds.subList(i * partition_num_per_txn, partition_num_per_txn + i * partition_num_per_txn);
            }
            for (int partitionId:list) {
                partitionDependency.put(partitionId,new ArrayList<>(list));
            }
        }
    }
    public void configure_store(){
        if(enable_states_partition){
            floor_interval= (int) Math.floor(NUM_ITEMS / (double) partition_num);//NUM_ITEMS / partition_num;
            partitioned_store =new FastZipfGenerator[partition_num];
            for (int i = 0; i < partition_num; i++) {
                partitioned_store[i] = new FastZipfGenerator(floor_interval, zipSkew, i * floor_interval);
            }
        }else{
            shared_store = new FastZipfGenerator(NUM_ITEMS, zipSkew,0);
        }
        partitionId_generator = new FastZipfGenerator(NUM_ITEMS,zipSkew,0);
    }
    public int key_to_partition(int key) {
        return (int) Math.floor((double) key / floor_interval);
    }
    protected  boolean verify(Set<Integer> keys, int partition_id, int number_of_partitions) {
        for (Object key : keys) {
            int i = (Integer) key;
            int pid = i / (floor_interval);

            boolean case1 = pid >= partition_id && pid <= partition_id + number_of_partitions;
            boolean case2 = pid >= 0 && pid <= (partition_id + number_of_partitions) % partition_num;

            if (!(case1 || case2)) {
                return false;
            }
        }
        return true;
    }
}
