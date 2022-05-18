package streamprocess.controller.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.topology.TopologyComponent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiStreamInFlightLog {
    private static final Logger LOG= LoggerFactory.getLogger(MultiStreamInFlightLog.class);
    private final HashMap<String,HashMap<String,InFlightLog>> InFightLogForStream;
    private class InFlightLog{
        private final HashMap<Integer, HashMap<Long,List<Object>>> InFlightEvents = new HashMap<>();
        private long currentOffset;
        public InFlightLog(ArrayList<Integer> executorID){
            for (int id:executorID){
                HashMap<Long,List<Object>> epochs = new HashMap<>();
                List<Object> events=new ArrayList<>();
                epochs.put(0L,events);
                InFlightEvents.put(id,epochs);
            }
            currentOffset=0;
        }
        public List<Object> getInFlightEventsByExecutor(int executeId, long offset){
            return InFlightEvents.get(executeId).get(offset);
        }
        public void addEvents(int executorID,Object o){
            InFlightEvents.get(executorID).get(currentOffset).add(o);
        }
        public void addEvents(Object o){
            for (HashMap<Long,List<Object>> epochs:InFlightEvents.values()){
                epochs.get(currentOffset).add(o);
            }
        }
        public void addEpoch(long offset){
            currentOffset = offset;
            for (HashMap<Long,List<Object>> epochs:InFlightEvents.values()){
                List<Object> events=new ArrayList<>();
                epochs.put(offset,events);
            }
        }
        public void cleanEpoch(long offset){
            for (HashMap<Long,List<Object>> epochs:InFlightEvents.values()){
                epochs.entrySet().removeIf(entry -> entry.getKey() < offset);
            }
        }
    }
    public MultiStreamInFlightLog(TopologyComponent op){
        InFightLogForStream = new HashMap<>();
        for (String streamId:op.get_childrenStream()){
            Map<TopologyComponent, Grouping> children = op.getChildrenOfStream(streamId);
            HashMap<String,InFlightLog> inFlightLogHashMap = new HashMap<>();
            for (TopologyComponent child:children.keySet()){
               InFlightLog inFlightLog = new InFlightLog(child.getExecutorIDList());
               inFlightLogHashMap.put(child.getId(),inFlightLog);
            }
            InFightLogForStream.put(streamId,inFlightLogHashMap);
        }
    }
    public List<Object> getInFightEventsForExecutor(String stream,String id, int executorID, long offset){
        return InFightLogForStream.get(stream).get(id).getInFlightEventsByExecutor(executorID,offset);
    }
    public void addEvent(int targetId, String stream,Object o){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.addEvents(targetId,o);
        }
    }
    public void addEvent(String stream,Object o){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.addEvents(o);
        }
    }
    public void addEpoch(long offset, String stream){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.addEpoch(offset);
        }
    }
    public void cleanEpoch(long offset, String stream){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.cleanEpoch(offset);
        }
    }
}
