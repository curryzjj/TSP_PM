package streamprocess.execution;

import System.Platform.Platform;
import System.util.Configuration;
import streamprocess.components.topology.MultiStreamComponent;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.input.InputStreamController;
import streamprocess.controller.output.MultiStreamOutputContoller;
import streamprocess.controller.output.OutputController;
import streamprocess.controller.output.partition.PartitionController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ExecutionGraph {
    public final Topology topology;
    private final InputStreamController global_tuple_scheduler;
    private final Configuration conf;
    final ArrayList<ExecutionNode> executionNodeArrayList;
    int vertex_id = 0;//Each executor has its unique vertex id.

    private ExecutionNode virtualNode;
    private ExecutionNode spout;
    private ExecutionNode sink;
    private boolean shared;//OutputController share by multi producers
    private boolean common;//OutputController shared by multi consumers
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
        int operator_numTasks=operator.getNumTasks();
        for(int i=0;i< operator_numTasks;i++){
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p);//Every executionNode obtain its unique vertex id..
            //set First or Last ExecutorNode
            if(i==operator_numTasks-1){
                vertex.setLast_executorOfBolt(true);
            }
            if(i==0){
                vertex.setFirst_executor(true);
                if(conf.getBoolean("profile",false)){
                    vertex.setNeedsProfile();
                }
            }
            addExecutor(vertex);
            operator.link_to_executor(vertex);//creates Operator->executor link
        }
    }
    private ExecutionNode addRecord(ExecutionNode e, TopologyComponent topo, Platform platform) {

        //creates executor->Operator link
        ExecutionNode vertex = new ExecutionNode(e, topo, platform);//Every executionNode obtain its unique vertex id..
        addExecutor(vertex);
        topo.link_to_executor(vertex);//creates Operator->executor link.
        return vertex;
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
         shared= conf.getBoolean("shared",false);
         common=conf.getBoolean("common",false);
         final ArrayList<ExecutionNode> executionNodeArrayList = getExecutionNodeArrayList();
         //buildTopology
         spout = executionNodeArrayList.get(0);
         sink = executionNodeArrayList.get(executionNodeArrayList.size() - 1);
         virtualNode = addVirtual(p);
         //create the Parent-Children executor link
         for(ExecutionNode executor:executionNodeArrayList){
             if(executor.isLeafNode()){
                 continue;
             }
             final TopologyComponent operator=executor.operator;
             for(String stream:operator.get_childrenStream()){
                 add(operator.getChildrenOfStream(stream).keySet(),executor,operator);
             }
         }
         for (ExecutionNode par : sink.operator.getExecutorList()) {
             par.getChildren().put(virtualNode.operator, new ArrayList<>());
             par.getChildrenOf(virtualNode.operator).add(virtualNode);
             virtualNode.getParents().putIfAbsent(sink.operator, new ArrayList<>());
             virtualNode.getParentsOf(sink.operator).add(par);
         }
         //build_streamController
         build_streamController(conf.getInt("batch",100));
         //loading statistics
         Loading(conf,p);
     }
     //used by the above setup
    private ExecutionNode addVirtual(Platform p){return null;}
    private void add(Set<TopologyComponent> children, ExecutionNode executor, TopologyComponent operator){
         for(TopologyComponent child:children){
             executor.getChildren().putIfAbsent(child,new ArrayList<>());
             final ArrayList<ExecutionNode> childrenExecutor=child.getExecutorList();
             for(ExecutionNode childExecutor:childrenExecutor){
                 executor.getChildrenOf(child).add(childExecutor);
                 childExecutor.getParents().putIfAbsent(operator,new ArrayList<>());
                 childExecutor.getParentsOf(operator).add(executor);
             }
         }
    }
    private void Loading(Configuration conf,Platform p){}
    private void build_streamController(int batch){
         if(!shared){
             for(ExecutionNode executor:executionNodeArrayList){//build sc for each executor who is not leaf mapping_node
                 if(executor.isLeafNode()){
                     continue;
                 }
                 //<output_streamId,<DownOpId,PC>> for each executor
                 HashMap<String,HashMap<String,PartitionController>> PCMaps=new HashMap<>();
                 for(String streamId:executor.operator.getOutput_streamsIds()){
                     HashMap<String,PartitionController> PClist=init_pc(streamId,executor.operator,batch,executor,common);
                     PCMaps.put(streamId,PClist);
                 }
                 OutputController sc=new MultiStreamOutputContoller((MultiStreamComponent) executor.operator,PCMaps);
                 executor.setController(sc);
             }
         }else{
             for(TopologyComponent operator:topology.getRecords().values()){
                 if(operator.isLeadNode()){
                     continue;
                 }
                 //<output_streamId,<DownOpId,PC>> for operator
                 HashMap<String,HashMap<String,PartitionController>> PCMaps=new HashMap<>();
                 for(String streamId:operator.getOutput_streamsIds()){
                     HashMap<String,PartitionController> PClist=init_pc(streamId,operator,batch,null,common);
                     PCMaps.put(streamId,PClist);
                 }
                 OutputController sc=new MultiStreamOutputContoller((MultiStreamComponent) operator,PCMaps);
                 for(ExecutionNode executor:operator.getExecutorList()){
                     executor.setController(sc);
                 }
             }
         }
    }
    //used by the build_streamController
    private HashMap<String, PartitionController> init_pc(String streamId,TopologyComponent srcOP,
                                                         int batch, ExecutionNode executor, boolean common){
         HashMap<String,PartitionController> PClist=new HashMap<>();//<childOp.Id,PartitionController>
         if(srcOP.getChildrenOfStream(streamId)!=null){
             for(TopologyComponent childOP:srcOP.getChildrenOfStream(streamId).keySet()){
                 HashMap<Integer,ExecutionNode> downExecutor_list=new HashMap<>();
                 for(ExecutionNode e:childOP.getExecutorList()){
                     downExecutor_list.put(e.getExecutorID(),e);
                 }
                 PClist.put(childOP.getId(),partitionController_create(srcOP,childOP,streamId,downExecutor_list,batch,executor,common));
             }
         }
         return PClist;
    }
    private PartitionController partitionController_create(TopologyComponent srcOP, TopologyComponent childOP,
                                                           String streamId, HashMap<Integer,ExecutionNode> downExexutor_list,
                                                           int batch, ExecutionNode executor,boolean common){
         //wait for the PartitionController
         return null;
    }
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

