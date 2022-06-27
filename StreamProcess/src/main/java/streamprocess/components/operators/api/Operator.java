package streamprocess.components.operators.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import System.constants.BaseConstants;
import System.util.Configuration;
import System.util.OsUtils;
import engine.Clock;
import engine.Database;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.FTManager;
import streamprocess.faulttolerance.checkpoint.emitMarker;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.msgs.Marker;
import streamprocess.execution.runtime.tuple.OutputFieldsDeclarer;
import streamprocess.faulttolerance.clr.CausalService;
import streamprocess.faulttolerance.clr.RecoveryDependency;

import static System.constants.BaseConstants.BaseField.TEXT;
import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;

public abstract class Operator implements Serializable{
    //some common operator
    public static final String map = "map";//Takes one element and produces one element. A map function that doubles the values of the input stream
    public static final String filter = "filter";//Evaluates a boolean function for each element and retains those for which the function returns true, e.g., A filter that filters out zero values:
    public static final String reduce = "reduce";//Combine multiple input data into one output data.
    public static final String w_apply = "w_apply";
    //end
    private static final long serialVersionUID = -7816511217365808709L;
    public static String flatMap = "flatMap";//Takes one element and produces zero, one, or more elements, e.g., A flatmap function that splits sentences to words:
    public static String w_join = "w_join";//Join two data streams on a given key and a common window.
    public static String union = "union";//Union of two or more data streams creating a new stream containing all the elements from all the streams. SimExecutionNode: If you union a data stream with itself you will GetAndUpdate each element twice in the resulting stream.
    public final Map<String, Double> input_selectivity;//input_selectivity used to capture multi-stream effect.
    public final Map<String, Double> output_selectivity;//output_selectivity can be > 1
    public final double branch_selectivity;
    private final boolean ByP;//Time by processing? or by input_event.
    private final double Event_frequency;

    public Map<String, Fields> getOutputFields() {
        return fields;
    }

    protected final Map<String, Fields> fields;
    public double read_selectivity;//the ratio of actual reading..
    public double loops = -1;//by default use argument loops.
    public boolean scalable = true;

    public TopologyContext context;
    public Clock clock;
    public Status status = null;
//    public OrderLock lock;//used for lock_ratio-based ordering constraint.
//    public OrderValidate orderValidate;
//    public transient TxnContext[] txn_context = new TxnContext[combo_bid_size];
    public transient Database db;//this is only used if the bolt is transactional bolt. DB is shared by all operators.
    public FTManager FTM;

    protected long checkpointId;
    protected boolean needcheckpoint;

    public boolean isCommit;
    public boolean replay=false;
    public boolean needWaitReplay = false;
    public int lostData=0;
    protected Object lock;
    //    public transient TxnContext txn_context;
    public boolean forceStop;
    public int fid = -1;//if fid is -1 it means it does not participate transactional process
    public String configPrefix = BaseConstants.BASE_PREFIX;
    protected OutputCollector collector;
    protected Configuration config;
    protected ExecutionNode executor;//who owns this operator
    Logger LOG;
    int partition_count_;
    int partition_id_;
    boolean Stateful = false;
    private double window = 1;//by default window fieldSize is 1, means per-tuple execution
    private double throughputResults = 0;

    /**
     * @param log
     * @param output_selectivity
     * @param branch_selectivity
     * @param read_selectivity
     * @param byP
     * @param event_frequency
     * @param window_size
     */
    Operator(Logger log, Map<String, Double> input_selectivity,
             Map<String, Double> output_selectivity, double branch_selectivity,
             double read_selectivity, boolean byP, double event_frequency, double window_size) {
        LOG = log;
        OsUtils.configLOG(LOG);
        if (input_selectivity == null) {
            this.input_selectivity = new HashMap<>();
            this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        } else {
            this.input_selectivity = input_selectivity;
        }
        if (output_selectivity == null) {
            this.output_selectivity = new HashMap<>();
            this.output_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        } else {
            this.output_selectivity = output_selectivity;
        }

        this.branch_selectivity = branch_selectivity;
        this.read_selectivity = read_selectivity;
        ByP = byP;
        Event_frequency = event_frequency;
        window = window_size;
        fields = new HashMap<>();
    }
    Operator(Logger log, boolean byP, double event_frequency, double w) {
        LOG = log;
        //OsUtils.configLOG(LOG);
        this.input_selectivity = new HashMap<>();
        this.output_selectivity = new HashMap<>();
        this.input_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.output_selectivity.put(DEFAULT_STREAM_ID, 1.0);
        this.branch_selectivity = 1;
        this.read_selectivity = 1;
        ByP = byP;
        Event_frequency = event_frequency;
        window = w;
        fields = new HashMap<>();
    }
    public void setStateful(){ Stateful = true;}
    public void display(){};//display something when the thread stop
    public OutputCollector getCollector() {
        return collector;
    }
    public TopologyContext getContext() { return context; }
    private void setContext(TopologyContext context) {
        this.context = context;
    }

    //Fields
    public void setFields(Fields fields) { this.fields.put(BaseConstants.BaseStream.DEFAULT, fields); }
    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        if(fields.isEmpty()){
            if(getDefaultFields()!=null){
                fields.put(BaseConstants.BaseStream.DEFAULT,getDefaultFields());
            }
            if(getDefaultStreamFields()!=null){
                fields.putAll(getDefaultStreamFields());
            }
        }
        for(Map.Entry<String,Fields> e:fields.entrySet()){
            declarer.declareStream(e.getKey(),e.getValue());
        }
    }
    //default fields
    protected Fields getDefaultFields() {//@define the output fields
        return new Fields(TEXT);
    }
    protected Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    //config
    public String getConfigPrefix() {
        return this.configPrefix;
    }
    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
    //exexutor
    public void setExecutionNode(ExecutionNode e) {
        this.executor = e;
    }
    public int getId() {
        return this.executor.getExecutorID();
    }
    //window
    public double getWindow() {
        return window;
    }
    public void setWindow(double window) {
        this.window = window;
    }

    public double getLoops() {
        return loops;
    }
    public double getResults() {
        return throughputResults;
    }
    public void setResults(double results) {
        this.throughputResults = results;
    }

    /**
     * This is the API to talk to actual thread.
     *
     * @param conf
     * @param context
     * @param collector
     */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.config=Configuration.fromMap(conf);
        setContext(context);
        this.collector=collector;
        base_initialize(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(),
                context.getGraph());
    }
    public void loadDB(TopologyContext context){
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }
    public void loadDB(int thread_Id, int thisTaskId, ExecutionGraph graph){
        //initialize in the Topology by the TableInitializer
        graph.topology.tableinitilizer.loadDB(thread_Id, this.context);
    }

    public void cleanup() {}
    public void callback(int callee, Marker marker){};

    /**
     * Base init will always be called.
     *
     * @param thread_Id
     * @param thisTaskId
     * @param graph
     * called by the prepare
     */
    private void base_initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        if(LOG==null){
            LOG= LoggerFactory.getLogger(Operator.class);
            LOG.info("The operator has no LOG, create a default one for it here.");
        }
        if(OsUtils.isMac()){
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        }else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }
        if(this instanceof emitMarker){
            if (status == null) {
                LOG.info("The operator" + executor.getOP() + " is declared as checkpointable " +
                        "but no state is initialized");
            } else {
                    status.source_status_ini(executor);
                    status.dst_status_init(executor);
            }
        }
        db = getContext().getDb();
        FTM = context.getFTM();
        initialize(thread_Id, thisTaskId, graph);
    }
    /**
     * This is the API to client application code.
     * This can be overwrite by specific operator to do some initialization work.
     *
     * @param thread_Id
     * @param thisTaskId
     * @param graph
     * called by the base_initialize
     */
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {}

    public Integer default_scale(Configuration conf) {
        return 1;
    }
    public int getFid() {
        return fid;
    }
    public boolean IsStateful() {
        return Stateful;
    }
    public void forceStop() {
        forceStop = true;
    }
    public double getEmpty() {
        return 0;
    }

    /**
     * forward_checkpoint implementation
     * save state of the operator with or without MMIO.
     * TODO: support exactly once in future.
     *
     * @param value    the value_list to be updated.
     * @param sourceId
     * @param marker
     */
    public boolean checkpoint_store(Serializable value,int sourceId,Marker marker){
        //implement after the transaction process
        return false;
    }
    /**
     * Simple forward the marker
     *
     * @param sourceId
     * @return
     */
    public boolean checkpoint_forward(int sourceId){
        //implement after the transaction process
        return false;
    }

    public void cleanEpoch(long offset){

    }
    public RecoveryDependency returnRecoveryDependency(){
        return null;
    }
    public ConcurrentHashMap<Integer, CausalService> returnCausalService(){
        return null;
     }

    public void loadInFlightLog(){}
    public void replayEvents() throws InterruptedException {}
}
