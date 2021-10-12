package streamprocess.controller.input;

import streamprocess.execution.ExecutionNode;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.optimization.model.STAT;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

/**
 * for an executor (except spout's executor), there's a receive queue
 * for *each* upstream executor's *each* stream output (if subscribed).
 * streamId, SourceId -> queue
 */

public abstract class InputStreamController implements Serializable {
    private final HashMap<String, HashMap<Integer, Queue>> RQ=new HashMap<>();
    protected Set<String> keySet;
    TreeSet<JumboTuple> tuples=new TreeSet<>();//temporarily holds all retrieved tuples.
    protected InputStreamController(){

    }
    public abstract JumboTuple fetchResults_inorder();
    public abstract Object fetchResults();
    public abstract Tuple fetchResults_single();
    public abstract JumboTuple fetchResults(int batch);//bypass STAT
    //fetch result
    protected Tuple fetchFromqueue_single(Queue queue){
        Tuple tuple;
        tuple=(Tuple) queue.poll();
        if(tuple != null){
            return tuple;
        }
        return null;
    }
    protected Object fetchFromqueue(Queue queue){
        Object tuple;
        tuple=queue.poll();
        if(tuple!=null){
            synchronized (queue){
                queue.notifyAll();
            }
            return tuple;
        }
        return null;
    }
    protected JumboTuple fetchFromqueue(Queue queue, int batch) {//bypass STAT
        JumboTuple tuple = (JumboTuple) fetchFromqueue(queue);
        if (tuple != null) {
            return tuple;
        }
        return null;
    }
    protected JumboTuple fetchFromqueue_inorder(Queue queue){
        if(!tuples.isEmpty()){
            return tuples.pollFirst();
        }
        final int size=queue.size();
        for(int i=0;i<size;i++){
            tuples.add((JumboTuple) fetchFromqueue(queue));
        }
        return tuples.pollFirst();
    }
    public JumboTuple fetchResults(ExecutionNode src,int batch){//bypass STAT
        for(String streamId:keySet){
            Queue queue=null;
            queue = getRQ().get(streamId).get(src.getExecutorID());
            if (queue == null) {
                continue;
            }
//            for (int i = 0; i < batch; i++) {
//                t[i] = fetchFromqueue((P1C1Queue) queue, stat, batch);

            return fetchFromqueue(queue, batch);
        }
        return null;
    }
    public HashMap<String, HashMap<Integer, Queue>> getRQ() {
        return RQ;
    }
    public HashMap<Integer, Queue> getReceive_queue(String streamId) {
        return RQ.get(streamId);
    }
    public void initialize() {
        keySet = RQ.keySet();
    }
    public synchronized void setReceive_queue(String streamId, int executorID, Queue q) {
        HashMap<Integer, Queue> integerP1C1QueueHashMap = RQ.get(streamId);
        if (integerP1C1QueueHashMap == null) {
            integerP1C1QueueHashMap = new HashMap<>();
        }
        integerP1C1QueueHashMap.put(executorID, q);
        RQ.put(streamId, integerP1C1QueueHashMap);
    }
}
