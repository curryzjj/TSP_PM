package streamprocess.execution;

import System.Platform.Platform;
import System.util.Configuration;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.input.InputStreamController;
import streamprocess.controller.output.PartitionController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ExecutionGraph {
    public final Topology topology;
    private final InputStreamController global_tuple_scheduler;
    private final Configuration conf;
    final ArrayList<ExecutionNode> executionNodeArrayList;
    int vertex_id = 0;//Each executor has its unique vertex id.

    private ExecutionNode virtualNode;
    private ExecutionNode spout;
    private ExecutionNode sink;
    private boolean shared;//share by multi producers
    private boolean common;//shared by mutlti consumers
    //some Constructors
    public ExecutionGraph(Topology topo, Map<String, Integer> parallelism, Configuration conf){
        executionNodeArrayList=new ArrayList<>();
        this.conf=conf;
        this.topology=topo;
        this.global_tuple_scheduler=topo.getScheduler();
        Configuration(this.topology, parallelism, conf, topology.getPlatform());
    }
    //end

    //Configure set the numTasks of the operator, and create the Operator->executor link
    private void addRecord(TopologyComponent operator, Platform p){
        //create executionNode and assign unique vertex id to it,each task has an executor
        int operator_numTasks=1;
        for(int i=0;i< operator_numTasks;i++){
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p);//Every executionNode obtain its unique vertex id..
            //set First or Last ExecutorNode
            addExecutor(vertex);
            //creates Operator->executor link
        }
    }
    private void Configuration(Topology topology, Map<String, Integer> parallelism, Configuration conf, Platform p){
        //set num of Tasks specified in operator
        for (TopologyComponent tr : topology.getRecords().values()) {
            addRecord(tr, p);
        }
        setup(conf,p);
    }
    //end

    //setup
    /**
     * this will set up partition partition (also the partition ratio)
     *
     * @param conf
     * @param p
     */
     private void setup(Configuration conf, Platform p){
         final ArrayList<ExecutionNode> executionNodeArrayList = getExecutionNodeArrayList();
         //buildTopology
         spout = executionNodeArrayList.get(0);
         sink = executionNodeArrayList.get(executionNodeArrayList.size() - 1);
         virtualNode = addVirtual(p);
         //create the Parent-Children executor link
         add();
         //build_streamController
         build_streamController(0);
         //loading statistics
         Loading(conf,p);
     }
     //used by the above setup
    private ExecutionNode addVirtual(Platform p){return null;}
    private void add(){}
    private void Loading(Configuration conf,Platform p){}
    private void build_streamController(int batch){}
    //used by the build_streamController
    private HashMap<String, PartitionController> init_pc(String streamId,TopologyComponent srcOP,int batch, ExecutionNode executor, boolean common){return null;}
    //end
    //end
    //end

    //get+add ExecutionNode getExecutionNodeArrayList
    void addExecutor(ExecutionNode e){ executionNodeArrayList.add(e);}
    public ExecutionNode getExecutionNode(int id){return executionNodeArrayList.get(id);}
    public ArrayList<ExecutionNode> getExecutionNodeArrayList() {
        return executionNodeArrayList;
    }
    //end
    //some public function
    public ExecutionNode getSpout() {
        return spout;
    }
    public ExecutionNode getSink() {
        return sink;
    }
    public ExecutionNode getvirtualGround() {
        return virtualNode;
    }
    public InputStreamController getGlobal_tuple_scheduler() {
        return global_tuple_scheduler;
    }
    public Integer getSinkThread() {
        return sink.getExecutorID();
    }
    public void build_inputSchedule(){}
    //end
}

