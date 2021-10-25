package applications.spout.transactional;

import System.tools.FastZipfGenerator;
import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.TransactionalSpout;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.execution.ExecutionGraph;

import java.util.LinkedHashSet;
import java.util.Set;

import static System.constants.BaseConstants.CCOptions.CCOption_TStream;
import static UserApplications.constants.PKConstants.Constant.SIZE_EVENT;

public class PKTransactionalSpout extends TransactionalSpout {
    private static final Logger LOG= LoggerFactory.getLogger(TransactionalSpout.class);
    private Set[] input_keys = new Set[10_000];
    private long[] p_bid;
    protected PKTransactionalSpout() {
        super(LOG);
        this.scalable=false;
    }

    @Override
    public Integer default_scale(Configuration conf) {//not sure
        return 1;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("TransactionalSpout initialize is being called");
        long start=System.nanoTime();
        taskId=getContext().getThisTaskId();
        //TODO:get from the configuration
        double skew=0.6;
        int size=40;
        keygenerator=new FastZipfGenerator(size,skew,0);
        ccOption=config.getInt("CCOption",0);
        tthread=config.getInt("tthread");
        checkpoint_interval_sec = config.getDouble("checkpoint");
        target_Hz = (int) config.getDouble("targetHz", 10000000);
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
        bid=0;
        p_bid=new long[tthread];
        for (int i=0;i<tthread;i++){//what is this for?
            p_bid[i]=0;
        }
        for (TopologyComponent children : this.context.getThisComponent().getChildrenOfStream().keySet()) {
            int numTasks = children.getNumTasks();
            total_children_tasks += numTasks;
        }
        //generator the key
        for (int k = 0; k < 10_000; k++) {
            Set<Integer> keys = new LinkedHashSet<>();
            for (int i = 0; i < SIZE_EVENT; i++) {
                int key = keygenerator.next();
                while (keys.contains(key)) {
                    key = keygenerator.next();
                }
                keys.add(key);
            }
            input_keys[k] = keys;
        }
        long end = System.nanoTime();
        LOG.info("spout prepare takes (ms):" + (end - start) / 1E6);
    }

    @Override
    public void nextTuple() throws InterruptedException {
        Set<Integer> keys=new LinkedHashSet<>();
        keys.addAll(input_keys[counter++%10_000]);
        if(ccOption==CCOption_TStream){
            if(control<target_Hz){//why?
                collector.emit_single(bid,keys);
                control++;
            }else{
                empty++;
            }
            forward_checkpoint(-1,bid,null);
        }
    }
}
