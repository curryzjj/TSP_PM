package streamprocess.execution;

import System.Platform.Platform;
import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.operators.executor.VirtualExecutor;
import streamprocess.components.topology.MultiStreamComponent;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.controller.input.InputStreamController;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.controller.input.scheduler.UniformedScheduler;
import streamprocess.controller.output.MultiStreamOutputContoller;
import streamprocess.controller.output.OutputController;
import streamprocess.controller.output.PartitionController;
import streamprocess.controller.output.partition.AllPartitionController;
import streamprocess.controller.output.partition.FieldsPartitionController;
import streamprocess.controller.output.partition.ShufflePartitionController;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static System.Constants.virtualType;

public class ExecutionGraph implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);
    public final Topology topology;
    private final InputStreamController global_tuple_scheduler;
    private final Configuration conf;
    final ArrayList<ExecutionNode> executionNodeArrayList;
    int vertex_id = 0;//Each executionNode has its unique vertex id.

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
    public ExecutionGraph(ExecutionGraph graph,Configuration conf){
        executionNodeArrayList=new ArrayList<>();
        this.conf=conf;
        topology=new Topology(graph.topology);
        topology.clean_executorInformation();
        global_tuple_scheduler=graph.global_tuple_scheduler;
        Configuration(graph,conf,topology.getPlatform());
    }
    public ExecutionGraph(ExecutionGraph graph, int compressRatio, Configuration conf){
        executionNodeArrayList=new ArrayList<>();
        this.conf=conf;
        topology= graph.topology;
        topology.clean_executorInformation();
        global_tuple_scheduler=getGlobal_tuple_scheduler();
        if (compressRatio == -1) {
            LOG.info("automatically decide the compress ratio");
            compressRatio = (int) Math.max(5, Math.ceil(graph.getExecutionNodeArrayList().size() / 36.0));
        }

        LOG.info("Creates the compressed graph with compressRatio:" + compressRatio);
        Configuration(topology, compressRatio, conf, topology.getPlatform());
    }
    //Configure set the numTasks of the operator, and create the Operator->executor link
    private void Configuration(Topology topology, Map<String, Integer> parallelism, Configuration conf, Platform p){
        //set num of Tasks specified in operator
        if(parallelism!=null){//implement in the schedulingPlan
            for(String tr:parallelism.keySet()){
                TopologyComponent record=topology.getRecord(tr);
                record.setNumTasks(parallelism.get(tr));
            }
        }
        for (TopologyComponent tr : topology.getRecords().values()) {
            addRecord(tr, p);
        }
        setup(conf,p);
    }
    private void Configuration(ExecutionGraph graph, Configuration conf, Platform p) {
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            if (!e.isVirtual()) {
                addRecord(e, topology.getRecord(e.operator.getId()), p);
            }
        }
        spout = executionNodeArrayList.get(0);
        sink = executionNodeArrayList.get(executionNodeArrayList.size() - 1);
        setup(conf, p);
    }
    private void Configuration(Topology topology, int compressRatio, Configuration conf, Platform p) {
        for (TopologyComponent tr : topology.getRecords().values()) {
            if (!tr.isLeadNode() && tr.toCompress) {
                addRecord_RebuildRelationships(tr, compressRatio, p);
            } else {
                if (!tr.toCompress) {
                    LOG.info("Exe:" + tr.getId() + "not able to allocate in last round, do not compress it.");
                }
                addRecord(tr, p);
            }
        }
        setup(conf, p);
    }
    //addRecord
    private void addRecord(TopologyComponent operator, Platform p){
        //create executionNode and assign unique vertex id to it,each task has an executionNode
        int operator_numTasks=operator.getNumTasks();
        for(int i=0;i< operator_numTasks;i++){
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p);//Every executionNode obtain its unique vertex id..
            //set First or Last ExecutionNode
            if(i==operator_numTasks-1){
                vertex.setLast_executorOfBolt(true);
            }
            if(i==0){//use the first executor as the profiling target
                vertex.setFirst_executor(true);
                if(conf.getBoolean("profile",false)){
                    vertex.setNeedsProfile();
                }
            }
            addExecutor(vertex);//add ExecutionNode in the list
            operator.link_to_executor(vertex);//creates Operator->executor link, add executionNode in the TopologyComponent ArrayList<ExecutionNode>
        }
    }
    private ExecutionNode addRecord(ExecutionNode e, TopologyComponent topo, Platform platform) {

        //creates executor->Operator link
        ExecutionNode vertex = new ExecutionNode(e, topo, platform);//Every executionNode obtain its unique vertex id..
        addExecutor(vertex);
        topo.link_to_executor(vertex);//creates Operator->executor link.
        return vertex;
    }
    private void addRecord_RebuildRelationships(TopologyComponent operator, int compressRatio, Platform p) {
        if (compressRatio < 1) {
            LOG.info("compressRatio must be greater than 1, and your setting:" + compressRatio);
            System.exit(-1);
        }
        //create executionNode and assign unique vertex id to it.
        for (int i = 0; i < operator.getNumTasks() / compressRatio; i++) {
            //creates executor->Operator link
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p, compressRatio);//Every executionNode obtain its unique vertex id..
            if (i == operator.getNumTasks() - 1) {
                vertex.setLast_executorOfBolt(true);
            }
            if (i == 0) {
                vertex.setFirst_executor(true);
            }
            addExecutor(vertex);
            operator.link_to_executor(vertex);//creates Operator->executor link.
        }
        //left-over
        int left_over = operator.getNumTasks() % compressRatio;
        if (left_over > 0) {
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p, left_over);//Every executionNode obtain its unique vertex id..
            vertex.setLast_executorOfBolt(true);
            addExecutor(vertex);
            operator.link_to_executor(vertex);//creates Operator->executor link.
        }
    }
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
         //build_OutputController
         build_streamController(conf.getInt("batch",100));
         //loading statistics wait for the STAT
         Loading(conf,p);
     }
     //used by the above setup
    private ExecutionNode addVirtual(Platform p){
        MultiStreamComponent virtual = new MultiStreamComponent("Virtual", virtualType, new VirtualExecutor(), 1, null, null, null);
        ExecutionNode virtualNode = new ExecutionNode(virtual, -2, p);
        addExecutor(virtualNode);
        virtual.link_to_executor(virtualNode);//creates Operator->executor link.

        return virtualNode;
    }
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
    private void Loading(Configuration conf,Platform p){}//impl after
    public void build_inputSchedule(){
         for(ExecutionNode executor:executionNodeArrayList){
             if(executor.isSourceNode()){
                 continue;
             }
             //Each thread has its own Brisk.execution.runtime.tuple scheduler, which can be customize for each bolt.
             if(!executor.hasScheduler()){
                 if (global_tuple_scheduler instanceof SequentialScheduler) {
                     executor.setInputStreamController(new SequentialScheduler());
                 } else if (global_tuple_scheduler instanceof UniformedScheduler) {
                     executor.setInputStreamController(new UniformedScheduler());
                 } else {
                     LOG.error("Unknown input scheduler!");
                 }
             }
         }
    }
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
                 if(operator.isLeafNode()){
                     continue;
                 }
                 //<output_streamId,<DownOpId,PC>> for operator
                 HashMap<String,HashMap<String,PartitionController>> PCMaps=new HashMap<>();
                 for(String streamId:operator.getOutput_streamsIds()){
                     HashMap<String,PartitionController> PClist=init_pc(streamId,operator,batch,null,common);
                     PCMaps.put(streamId,PClist);
                 }
                 OutputController sc=new MultiStreamOutputContoller((MultiStreamComponent) operator,PCMaps);
                 sc.setShared();
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
                                                           String streamId, HashMap<Integer,ExecutionNode> downExecutor_list,
                                                           int batch, ExecutionNode executor,boolean common){
        Grouping g=srcOP.getGrouping_to_downstream(childOP.getId(), streamId);
        //implement other partition controller in the future
        if(g.isAll()){
            return new AllPartitionController(srcOP,childOP,downExecutor_list,batch,executor,common,conf.getBoolean("profile",false),conf);
        }else if(g.isFields()){
            return new FieldsPartitionController(srcOP,childOP,downExecutor_list, srcOP.get_output_fields(streamId),g.getFields(),batch,executor,common,conf.getBoolean("profile",false),conf);
        }else if(g.isShuffle()){
            return new ShufflePartitionController(srcOP, childOP
                    , downExecutor_list, batch, executor, common, conf.getBoolean("profile", false), conf);
        } else {
            LOG.info("create partition controller error: not suppourted yet");
            return null;
        }
    }
    //get+add ExecutionNode getExecutionNodeArrayList
    void addExecutor(ExecutionNode e){ executionNodeArrayList.add(e);}
    public ExecutionNode getExecutionNode(int id){return executionNodeArrayList.get(id);}
    public ArrayList<ExecutionNode> getExecutionNodeArrayList() {
        return executionNodeArrayList;
    }
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
    public ArrayList<ExecutionNode> sort() {
        return getExecutionNodeArrayList();
    }
    //display
    public ArrayList<ExecutionNode> display_ExecutionNodeList(){
        return executionNodeArrayList;
    }
}

