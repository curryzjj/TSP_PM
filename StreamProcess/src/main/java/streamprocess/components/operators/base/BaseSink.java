package streamprocess.components.operators.base;

import System.constants.BaseConstants;
import System.util.ClassLoaderUtils;
import System.util.Configuration;
import applications.sink.formatter.BasicFormatter;
import applications.sink.formatter.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.operators.api.AbstractBolt;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.faulttolerance.checkpoint.Checkpointable;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public abstract class BaseSink extends BaseOperator implements Checkpointable {
    private static final Logger LOG= LoggerFactory.getLogger(BaseSink.class);
    protected static ExecutionGraph graph;
    protected int thisTaskId;
    boolean isSINK=true;
    protected static final int max_num_msg=(int) 1E5;
    private BaseSink(Logger log){super(log);}
    BaseSink(Map<String, Double> input_selectivity, double read_selectivity) {
        super(LOG, input_selectivity, null, (double) 1, read_selectivity);
    }
    public BaseSink(Map<String, Double> input_selectivity) {
        this(input_selectivity, 0);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thisTaskId=thisTaskId;
        BaseSink.graph=graph;
        String formatterClass=config.getString(getConfigKey(),null);
        Formatter formatter;
        //TODO:initialize formatter
        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }

        formatter.initialize(Configuration.fromMap(config), getContext());
        if(thisTaskId==graph.getSink().getExecutorID()){
            isSINK=true;
        }
    }
    protected void registerRecovery() throws InterruptedException {
        this.lock=this.getContext().getRM().getLock();
        synchronized (lock){
            this.getContext().getRM().boltRegister(this.executor.getExecutorID());
            lock.notifyAll();
        }
        synchronized (lock){
            while(!isCommit){
                LOG.info(this.executor.getOP_full()+" is waiting for the Recovery");
                lock.wait();
            }
        }
        isCommit=false;
    }
    protected abstract Logger getLogger();

    @Override
    protected Fields getDefaultFields() {
        return new Fields("");
    }
    private String getConfigKey() {
        return String.format(BaseConstants.BaseConf.SINK_FORMATTER, configPrefix);
    }
    protected void killTopology(){
        LOG.info("Killing application");
    }

    @Override
    public boolean checkpoint(int counter) throws InterruptedException, BrokenBarrierException {
        return false;
    }

    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker, String msg) throws InterruptedException {

    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException {

    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker, String msg) throws InterruptedException {

    }

    @Override
    public void ack_checkpoint(Marker marker) {

    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }
}
