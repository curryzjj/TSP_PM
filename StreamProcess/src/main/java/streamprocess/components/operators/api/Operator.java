package streamprocess.components.operators.api;

import java.util.HashMap;
import java.util.Map;

import System.constants.BaseConstants;
import System.util.Configuration;
import org.slf4j.Logger;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.OutputCollector;
import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.Marker;
import streamprocess.execution.runtime.tuple.OutputFieldsDeclarer;

import static System.Constants.DEFAULT_STREAM_ID;
import static System.constants.BaseConstants.BaseField.TEXT;

public abstract class Operator {
    //some common operator
    public static final String map = "map";//Takes one element and produces one element. A map function that doubles the values of the input stream
    //end
    public final Map<String, Double> input_selectivity;//used to capture multi-stream
    public final Map<String, Double> output_selectivity;//output
    public final double branch_selectivity;
    public double read_selectivity;//the ratio of actual reading
    public TopologyContext context;//This object provides information about the component's place within the StreamProcess.topology, such as Task ids, inputs and outputs, etc.
    public String configPrefix = BaseConstants.BASE_PREFIX;
    public double loops = -1;//by default use argument loops.

    private final boolean ByP;//Time by processing? or by input_event.
    private final double Event_frequency;
    private double window;
    private double results = 0;//???

    protected final Map<String, Fields> fields; //??
    protected OutputCollector collector;
    protected ExecutionNode executor;//who owns this Operator
    protected Configuration config;

    boolean Stateful = false;//stateful or stateless operator
    public boolean scalable = true;

    Logger LOG;
    public int fid =-1;
    public boolean forceStop;

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
   //     OsUtils.configLOG(LOG);
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
    public void display(){};//??
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
    public void declareOutputFields(OutputFieldsDeclarer declarer){}
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
        return results;
    }
    public void setResults(double results) {
        this.results = results;
    }

    /**
     * This is the API to talk to actual thread.
     *
     * @param conf
     * @param context
     * @param collector
     */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {}
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector){}
    public void loadDB(int thread_Id, int thisTaskId, ExecutionGraph graph){}

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
    private void base_initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {}

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



    /**
     * Simple forward the marker
     *
     * @param sourceId
     * @return
     */

}
