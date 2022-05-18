package streamprocess.controller.output;

import System.util.Configuration;
import System.util.DataTypes.StreamValues;
import System.util.OsUtils;
import org.jctools.queues.MpscArrayQueue;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.components.topology.TopologyContext;
import streamprocess.controller.queue.MPSCController;
import streamprocess.controller.queue.QueueController;
import streamprocess.controller.queue.SPSCController;
import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.collector.BIDGenerator;
import streamprocess.execution.runtime.collector.Meta;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.execution.runtime.tuple.msgs.*;
import streamprocess.execution.runtime.tuple.Message;

import java.io.Serializable;
import java.util.*;

public abstract class PartitionController implements IPartitionController,Serializable{
    private static final long serialVersionUID = 28L;
    private static final int SPIN_TRIES=100;
    private static boolean profile;
    protected final int batch;
    protected final HashMap<Integer,Double> partition_ratio=new HashMap<>();//what this for???? for Load balancing？？？
    protected final HashMap<Integer, ExecutionNode> downExecutor_list;
    protected final int downTaskSize;
    protected final ArrayList<Integer> extendedTargetId=new ArrayList<>();
    final TopologyComponent childOP;
    private final ExecutionNode executionNode;
    private final Logger LOG;
    private final QueueController controller;
    private final Collections[] collections;
    protected final TopologyContext[] context;
    protected Integer[] targetTasks;
    private int firt_executor_Id;
    int threashold;
    /**
     * @param operator
     * @param childOP
     * @param downExecutor_list
     * @param batch_size
     * @param executionNode  if this is null, it is a shared PC, otherwise, it is unique to each executor
     * @param common
     * @param log
     * @param profile
     * @param conf
     */
    protected PartitionController(TopologyComponent operator, TopologyComponent childOP,
                                  HashMap<Integer,ExecutionNode> downExecutor_list, int batch_size, ExecutionNode executionNode, boolean common, Logger log, boolean profile, Configuration conf){
        this.childOP=childOP;
        this.downExecutor_list=downExecutor_list;
        this.batch=batch_size;
        this.executionNode=executionNode;
        LOG=log;
        //for each output stream, there could be different executors to listen on
        int sumweight=0;
        for(ExecutionNode consumer: downExecutor_list.values()){
            sumweight+=consumer.compressRatio;
        }
        for(Map.Entry<Integer,ExecutionNode> e:downExecutor_list.entrySet()){
            partition_ratio.put(e.getKey(),(e.getValue().compressRatio/(double)sumweight));
        }
        Set<Integer> setID= downExecutor_list.keySet();
        targetTasks=setID.toArray(new Integer[setID.size()]);
        updateExtendedTargetId();
        downTaskSize=targetTasks.length;
        final ExecutionNode first=operator.getExecutorList().get(0);
        firt_executor_Id=first.getExecutorID();
        if(executionNode==null){//shared PC
            collections=new Collections[operator.getExecutorList().size()];
            context=new TopologyContext[operator.getExecutorList().size()];
            for(ExecutionNode src:operator.getExecutorList()){
                collections[(src.getExecutorID()-firt_executor_Id)]=new Collections(src.getExecutorID(),downExecutor_list,batch_size);
            }
            LOG.trace("MPSC implementation -- Queue is shared among multiple executors of the same producer.");
            controller=new MPSCController(downExecutor_list);
        }else{
            firt_executor_Id = executionNode.getExecutorID();
            collections = new Collections[1];
            context = new TopologyContext[1];
            collections[0] = new Collections(firt_executor_Id, downExecutor_list, batch_size);
            LOG.trace("SPSC implementation -- Queue is unique to each producer and consumer");
            controller = new SPSCController(downExecutor_list);
        }
        OsUtils.configLOG(LOG);
        PartitionController.profile=profile;
        int queue_size_per_core;
        queue_size_per_core = 100000000;
        threashold = queue_size_per_core - 1;//leave one space for watermark filling!
    }
    //use the method of the controller
    public void allocate_queue(boolean linked, int desired_elements_epoch_per_core) {
        controller.allocate_queue(linked, desired_elements_epoch_per_core);
    }
    public boolean isEmpty() {
        return controller.isEmpty();
    }
    public Queue get_queue(int executorID) {
        return controller.get_queue(executorID);
    }
    //public method
    public TopologyContext[] getContext() {
        return context;
    }
    public void setContext(int srcId, TopologyContext context) {
        this.context[srcId - firt_executor_Id] = context;
    }
    public HashMap<Integer, ExecutionNode> getDownExecutor_list() {
        return downExecutor_list;
    }
    public Double getPartition_ratio(int executorID) {
        return partition_ratio.get(executorID);
    }
    //create message
    private Marker package_marker(String streamId, Marker marker,String msg) {
        return new Marker(streamId, marker.timeStampNano, marker.msgId, marker.getMyiteration(),msg);
    }
    private Marker package_marker(String streamId, long timestamp, long bid, int myiteration, String msg) {
        return new Marker(streamId, timestamp, bid, myiteration,msg);
    }
    private GeneralMsg package_message(String streamId, Object... msg) {
        return new GeneralMsg<>(streamId, msg);
    }
    private GeneralMsg package_message(String streamId, StreamValues msg) {
        return new GeneralMsg<>(streamId, msg);
    }
    private IntDoubleDoubleMsg package_message(String streamId, int deviceID, double nextDouble, double movingAvergeInstant) {
        return new IntDoubleDoubleMsg(streamId, deviceID, nextDouble, movingAvergeInstant);
    }
    private Message package_message(String streamId, char[] msg) {
        return new StringMsg(streamId, msg);
    }
    private Message package_message(String streamId, char[] key, long value) {
        return new StringLongMsg(streamId, key, value);
    }
    protected Tuple create_marker(int srcId, String streamId, long timestamp, long bid, Marker marker) {
        return (new Tuple((int) bid, srcId, context[srcId - firt_executor_Id], marker,streamId));
    }
    //help method
    public void updateExtendedTargetId() {
    }
    private int applyWaitMethod(int counter) {
        if (0 == counter) {
            Thread.yield();
        } else {
            --counter;
        }
        return counter;
    }
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, Double> entry : partition_ratio.entrySet()) {
            sb.append("key: ").append(entry.getKey()).append("value_list: ").append(entry.getValue());
        }
        return sb.toString();
    }
    //actually offer method
    private boolean bounded_offer(Queue queue,final Object e){
        do{
        if (((MpscArrayQueue) queue).offerIfBelowThreshold(e,threashold)) {
                return true;
            }
            int timestamp_counter=SPIN_TRIES;
            applyWaitMethod(timestamp_counter);
       } while(!Thread.interrupted());
        return true;
    }
    private boolean nonbounded_offer(Queue queue, final Object e) {
        return queue.offer(e);
    }
    private boolean marker_offer(Queue queue, final Object e) {
        do {
            if (queue.offer(e)) {// it should always success
                return true;
            }
            int timestamp_counter = SPIN_TRIES;
            applyWaitMethod(timestamp_counter);
        } while (!Thread.interrupted());
        return true;
    }
    private boolean _offer(Object tuple,int targetId){
        Queue queue=get_queue(targetId);
        return bounded_offer(queue,tuple);
    }
    private boolean _try_offer(Object tuple,int targetId){
        Queue queue=get_queue(targetId);
        return nonbounded_offer(queue,tuple);
    }
    private boolean _offer_marker(Object tuple, int targetId) {
        Queue queue = get_queue(targetId);
        return marker_offer(queue, tuple);
    }
    //implement sequential emit in the future
    private boolean _inorder_offer(Object tuple,long bid,LinkedList<Long> gap,int targetId){
        return true;
    }
    //offer_bid method
    protected boolean offer_bid(int srcId,int targetId,String streamId,Object... output){
        JumboTuple tuple=collections[srcId-firt_executor_Id].add_bid(targetId,streamId,context[srcId-firt_executor_Id],output);
        if(tuple!=null){
            return _offer(tuple,targetId);
        }
        return false;
    }
    protected boolean offer_bid(int srcId,int targetId,String streamId,StreamValues output){
        JumboTuple tuple=collections[srcId-firt_executor_Id].add_bid(targetId,streamId,context[srcId-firt_executor_Id],output);
        if(tuple!=null){
            return _offer(tuple,targetId);
        }
        return false;
    }
    protected boolean offer_bid(int srcId,int targetId,String streamId,char[] output){
        JumboTuple tuple=collections[srcId-firt_executor_Id].add_bid(targetId,streamId,context[srcId-firt_executor_Id],output);
        if(tuple!=null){
            return _offer(tuple,targetId);
        }
        return false;
    }
    //offer method
    protected boolean offer(int srcId,int targetId,String streamId,long bid,Object... output){
        JumboTuple tuple=collections[srcId-firt_executor_Id].add(targetId,streamId,bid,context[srcId-firt_executor_Id],output);
        if(tuple!=null){
            return _offer(tuple,targetId);
        }
        return false;
    }
    protected boolean offer(int srcId, int targetId, String streamId, long bid, Object output) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, bid, context[srcId - firt_executor_Id], output);
        if (tuple != null) {
            return _offer(tuple, targetId);
        }
        return false;
    }
    protected boolean offer(int srcId, int targetId, String streamId, long bid, StreamValues output) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, bid, context[srcId - firt_executor_Id], output);
        if (tuple != null) {
            return _offer(tuple, targetId);
        }
        return false;
    }
    protected boolean offer(int srcId, int targetId, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, bid, context[srcId - firt_executor_Id], deviceID, nextDouble, movingAvergeInstant);
        if (tuple != null) {
            return _offer(tuple, targetId);
        }
        return false;
    }
    protected boolean offer(int srcId, int targetId, String streamId, long bid, char[] output) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, bid, context[srcId - firt_executor_Id], output);
        if (tuple != null) {
            return _offer(tuple, targetId);
        }
        return false;
    }
    protected boolean offer(int srcId, int targetId, String streamId, char[] key, long value) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, 0, context[srcId - firt_executor_Id], key, value);//does not care order. set bid to 0.
        if (tuple != null) {
            return _offer(tuple, targetId);
        }
        return false;
    }
    protected boolean offer(int srcId, int targetId, String streamId, char[] key, long value, long bid, long TimeStamp) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, 0, context[srcId - firt_executor_Id], key, value, bid, TimeStamp);//does not care order. set bid to 0.
        if (tuple != null) {
            return _offer(tuple, targetId);
        }
        return false;
    }
    protected boolean offer(int srcId, int targetId, String streamId, long bid, char[] key, long value) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, bid, context[srcId - firt_executor_Id], key, value);
        if (tuple != null) {
            return _offer(tuple, targetId);
        }
        return false;
    }
    //offer_inorder method
    protected boolean offer_inorder(int srcId,int targetId,String streamId,long bid,LinkedList<Long> gap,Object... output){
        JumboTuple tuple=collections[srcId-firt_executor_Id].add_inorder(targetId,streamId,bid,gap,context[srcId-firt_executor_Id],output);
        if(tuple!=null){
         _inorder_offer(tuple,tuple.getBID(),gap,targetId);
        }
        return false;
    }
    protected boolean offer_inorder(int srcId, int targetId, String streamId, long bid, LinkedList<Long> gap, char[] output) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add_inorder(targetId, streamId, bid, gap, context[srcId - firt_executor_Id], output);
        if (tuple != null) {
            _inorder_offer(tuple, tuple.getBID(), gap, targetId);

        }
        return false;
    }
    protected boolean offer_inorder(int srcId, int targetId, String streamId, long bid, LinkedList<Long> gap, StreamValues output) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add_inorder(targetId, streamId, bid, gap, context[srcId - firt_executor_Id], output);
        if (tuple != null) {
            return _inorder_offer(tuple, tuple.getBID(), gap, targetId);
        }
        return false;
    }
    protected boolean offer_inorder_single(int srcId, int targetId, String streamId, long bid, LinkedList<Long> gap, StreamValues output) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add_inorder_single(targetId, streamId, bid, gap, context[srcId - firt_executor_Id], output);
        _inorder_offer(tuple, tuple.getBID(), gap, targetId);
        return false;
    }
    protected boolean offer_inorder_push(int srcId, int targetId, String streamId, long bid, LinkedList<Long> gap) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].get_inorder(targetId, streamId, bid);
        if (tuple != null) {
            return _inorder_offer(tuple, tuple.getBID(), gap, targetId);
        }
        return false;
    }
    //try_offer method
    protected boolean try_offer(int srcId,int targetId,String streamId,Object... output){
        JumboTuple tuple=collections[srcId-firt_executor_Id].add(targetId,streamId,0,context[srcId-firt_executor_Id],output);//does not care order. set bid to 0.
        if(tuple!=null){
            _try_offer(tuple, targetId);
        }
        return false;
    }
    protected boolean try_offer(int srcId, int targetId, String streamId, char[] key, long value) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, 0, context[srcId - firt_executor_Id], key, value);//does not care order. set bid to 0.
        if (tuple != null) {
            _try_offer(tuple, targetId);
        }
        return false;
    }
    protected boolean try_offer(int srcId, int targetId, String streamId, char[] output) {
        JumboTuple tuple = collections[srcId - firt_executor_Id].add(targetId, streamId, 0, context[srcId - firt_executor_Id], output);//does not care order. set bid to 0.
        if (tuple != null) {
            return _try_offer(tuple, targetId);
        }
        return false;
    }
    //force_offer method
    protected boolean force_offer(int srcId,int targetId,String streamId,long bid,Object... output){
        return _offer(new Tuple(bid,srcId,context[srcId-firt_executor_Id],package_message(streamId,output),streamId),targetId);
    }
    protected boolean force_offer(int srcId, int targetId, String streamId, long msg_id, long[] bid, Object... output) {
        return _offer(new Tuple(msg_id, bid, srcId, context[srcId - firt_executor_Id], package_message(streamId, output)), targetId);
    }
    protected boolean force_offer(int srcId, int targetId, String streamId, long bid, char[] output) {
        return _offer(new Tuple(bid, srcId, context[srcId - firt_executor_Id], package_message(streamId, output),streamId), targetId);
    }
    protected boolean force_offer(int srcId, int targetId, String streamId, long bid, StreamValues output) {
        return _offer(new Tuple(bid, srcId, context[srcId - firt_executor_Id], package_message(streamId, output),streamId), targetId);
    }
    //offer_marker method
    protected boolean offer_marker(int srcId, int targetId, String streamId, long bid, Marker marker) {
        Tuple marker_tuple = (new Tuple(bid, srcId, context[srcId - firt_executor_Id], marker,streamId));
        return _offer_marker(marker_tuple, targetId);
    }
    protected boolean offer_create_marker(Tuple marker_tuple, int targetId) {
        return _offer_marker(marker_tuple, targetId);
    }
    //emit_marker uses the offer_maker method
    public int marker_boardcast(Meta meta, String streamId,long bid,Marker marker){
        for (int target : targetTasks) {
            offer_marker(meta.src_id, target, streamId, bid, marker);
        }
        return targetTasks.length;
    }
    public int create_marker_single(Meta meta, String streamId, long timestamp, long bid, int myiteration,String msg) {
        Tuple marker = create_marker(meta.src_id, streamId, timestamp, bid, package_marker(streamId, timestamp, bid, myiteration,msg));
        offer_create_marker(marker, targetTasks[0]);//only send to the first instance.
        return targetTasks.length;
    }
    public int create_marker_boardcast(Meta meta, String streamId, long timestamp, long bid, int myiteration,String msg) {
        Tuple marker = create_marker(meta.src_id, streamId, timestamp, bid, package_marker(streamId, timestamp, bid, myiteration,msg));
        for (int target : targetTasks) {
            offer_create_marker(marker, target);
        }
        return targetTasks.length;
    }

    public void clean(){
        this.controller.cleanAll();
    };

    //tuple collections
    class Collections implements Serializable {
        private static final long serialVersionUID = 29L;
        final int batch_size;
        final int src_Id;
        final int[] pointer;
        private volatile JumboTuple[] buffers;//maintains a list of JumboTuple for each consumer
        private int base = Integer.MAX_VALUE;
        Collections(int src_Id, HashMap<Integer, ExecutionNode> DownExecutor_list, int batch_size) {

            this.batch_size = batch_size;
            pointer = new int[DownExecutor_list.size()];
            buffers = new JumboTuple[DownExecutor_list.size()];

            for (int e : DownExecutor_list.keySet()) {
                if (e < base) {
                    base = e;
                }
            }
            for (int e : DownExecutor_list.keySet()) {
                pointer[e - base] = 0;
            }
            this.src_Id = src_Id;
        }
        private JumboTuple getTuple(final int p, final int index) {
            if (p + 1 == batch_size) {//batch is full
                pointer[index] = 0;
                return buffers[index];
            } else {
                pointer[index]++;
                return null;
            }
        }
        private JumboTuple getTuple_single(final int index) {
            return buffers[index];
        }
        private JumboTuple getTuple_Inorder(final int p, final int index) {
            if (p != 0) {
                return buffers[index];
            }
            return null;
        }

        /**
         * TODO: Current design does not allow cascading out-of-order processing.
         *
         * @param targetId
         * @param streamId
         * @param bid
         * @param context
         * @param value
         * @return
         */
        //add used in the offer,try_offer
        JumboTuple add(int targetId, String streamId, long bid, TopologyContext context, Object... value) {
            if (value.length == 1) {
                if (value[0] instanceof char[]) {
                    return add(targetId, streamId, bid, context, (char[]) value[0]);
                }
            }
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, value));
            return getTuple(p, index);
        }
        JumboTuple add(int targetId, String streamId, long bid, TopologyContext context, char[] value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, value));
            return getTuple(p, index);
        }
        JumboTuple add(int targetId, String streamId, long bid, TopologyContext context, Object value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, value));
            return getTuple(p, index);
        }
        JumboTuple add(int targetId, String streamId, long bid, TopologyContext context, StreamValues value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, value));
            return getTuple(p, index);
        }
        JumboTuple add(int targetId, String streamId, long bid, TopologyContext context, int deviceID, double nextDouble, double movingAvergeInstant) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, deviceID, nextDouble, movingAvergeInstant));
            return getTuple(p, index);
        }
        JumboTuple add(int targetId, String streamId, long bid, TopologyContext context, char[] key, long value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, key, value));
            return getTuple(p, index);
        }
        //end
        //add_inorder used in the offer_inorder
        JumboTuple add_inorder(int targetId, String streamId, long bid, LinkedList<Long> gap, TopologyContext context, Object... value) {
            final int index = targetId - base;
            if (pointer[index] == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            } else {
                long cbid = buffers[index].getBID();
                if (bid != cbid) {//different bid comes.
                    buffers[index].length = pointer[index];
                    _inorder_offer(buffers[index], cbid, gap, targetId);//enforce emit a partial-complete tuple. It is guaranteed that this tuple will have smaller batch id
                    buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
                    pointer[index] = 0;
                }
            }
            buffers[index].add(pointer[index], package_message(streamId, value));
            if (pointer[index] + 1 == batch_size) {//batch is full
                pointer[index] = 0;
                return buffers[index];
            } else {
                pointer[index]++;
                return null;
            }
        }
        JumboTuple add_inorder(int targetId, String streamId, long bid, LinkedList<Long> gap, TopologyContext context, char[] value) {
            final int index = targetId - base;
            if (pointer[index] == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            } else {
                long cbid = buffers[index].getBID();
                if (bid != cbid) {//different bid comes.
                    buffers[index].length = pointer[index];
                    _inorder_offer(buffers[index], cbid, gap, targetId);//enforce emit a partial-complete tuple. It is guaranteed that this tuple will have smaller batch id
                    buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
                    pointer[index] = 0;

                }
            }
            buffers[index].add(pointer[index], package_message(streamId, value));

            if (pointer[index] + 1 == batch_size) {//batch is full
                pointer[index] = 0;
                return buffers[index];
            } else {
                pointer[index]++;
                return null;
            }

        }
        JumboTuple add_inorder(int targetId, String streamId, long bid, LinkedList<Long> gap, TopologyContext context, StreamValues value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {//first tuple comes.
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            } else {
                long cbid = buffers[index].getBID();
                if (bid != cbid) {//different bid comes.
                    buffers[index].length = pointer[index];
                    _inorder_offer(buffers[index], cbid, gap, targetId);//enforce emit a partial-complete tuple. It is guaranteed that this tuple will have smaller batch id
                    buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
                    pointer[index] = 0;
                }
            }
            buffers[index].add(p, package_message(streamId, value));
            return getTuple(p, index);
        }
        JumboTuple add_inorder_single(int targetId, String streamId, long bid, LinkedList<Long> gap, TopologyContext context, StreamValues value) {
            final int index = targetId - base;
            buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            buffers[index].length = 1;
            buffers[index].add(0, package_message(streamId, value));
            return getTuple_single(index);
        }
        JumboTuple get_inorder(int targetId, String streamId, long bid) {
            final int index = targetId - base;
            final int p = pointer[index];
            return getTuple_Inorder(p, index);
        }
        //end
        //add_bid used in the offer_bid
        JumboTuple add_bid(int targetId, String streamId, TopologyContext context, Object... value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {
                long bid = BIDGenerator.getInstance().getAndIncrement();
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
                LOG.info("A tuple with bid: " + bid + " created @ " + DateTime.now());
            }
            buffers[index].add(p, package_message(streamId, value));
            if (p + 1 == batch_size) {
                pointer[index] = 0;
                return buffers[index];
            } else {
                pointer[index]++;
                return null;
            }
        }
        JumboTuple add_bid(int targetId, String streamId, TopologyContext context, StreamValues value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {
                buffers[index] = new JumboTuple(src_Id, BIDGenerator.getInstance().getAndIncrement(), batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, value));
            if (p + 1 == batch_size) {
                pointer[index] = 0;
                return buffers[index];
            } else {
                pointer[index]++;
                return null;
            }
        }
        JumboTuple add_bid(int targetId, String streamId, TopologyContext context, char[] value) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {
                long bid = BIDGenerator.getInstance().getAndIncrement();
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_message(streamId, value));
            if (p + 1 == batch_size) {
                pointer[index] = 0;
                return buffers[index];
            } else {
                pointer[index]++;
                return null;
            }
        }
        //end
        JumboTuple spout_add_marker(int targetId, String streamId, long timestamp, long bid, int myiteration, TopologyContext context,String msg) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {
                buffers[index] = new JumboTuple(src_Id, BIDGenerator.getInstance().getAndIncrement(), batch_size, context);
            }
            buffers[index].add(p, package_marker(streamId, timestamp, bid, myiteration,msg));
            buffers[index].length = p + 1;
            pointer[index] = 0;
            return buffers[index];
        }

        JumboTuple add_marker(int targetId, String streamId, long bid, Marker marker, TopologyContext context,String msg) {
            final int index = targetId - base;
            final int p = pointer[index];
            if (p == 0) {
                buffers[index] = new JumboTuple(src_Id, bid, batch_size, context);
            }
            buffers[index].add(p, package_marker(streamId, marker,msg));
            buffers[index].length = p + 1;
            pointer[index] = 0;
            return buffers[index];
        }
    }
}
