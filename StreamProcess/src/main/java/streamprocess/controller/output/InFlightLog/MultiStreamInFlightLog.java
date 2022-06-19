package streamprocess.controller.output.InFlightLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.topology.TopologyComponent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MultiStreamInFlightLog {
    private static final Logger LOG= LoggerFactory.getLogger(MultiStreamInFlightLog.class);
    //<StreamID,<OperatorId,InFlightLog>>
    private final ConcurrentHashMap<String,ConcurrentHashMap<String,InFlightLog>> InFightLogForStream;
    public MultiStreamInFlightLog(TopologyComponent op){
        InFightLogForStream = new ConcurrentHashMap<>();
        for (String streamId:op.get_childrenStream()){
            Map<TopologyComponent, Grouping> children = op.getChildrenOfStream(streamId);
            ConcurrentHashMap<String,InFlightLog> inFlightLogHashMap = new ConcurrentHashMap<>();
            for (TopologyComponent child:children.keySet()){
               InFlightLog inFlightLog = new InFlightLog(child.getExecutorIDList());
               inFlightLogHashMap.put(child.getId(),inFlightLog);
            }
            InFightLogForStream.put(streamId, inFlightLogHashMap);
        }
    }
    public BatchEvents getInFightEventsByOffset(String stream,String id,long offset){
        return InFightLogForStream.get(stream).get(id).getInFlightEventsByOffset(offset);
    }
    public HashMap<Long, BatchEvents> getInFlightEvents(String stream, String id, long offset) {
        HashMap<Long,BatchEvents> InFlightEvents = new HashMap<>();
        for (Map.Entry<Long,BatchEvents> e:InFightLogForStream.get(stream).get(id).InFlightEvents.entrySet()) {
            if (e.getKey() >= offset){
                InFlightEvents.put(e.getKey(),e.getValue());
            }
        }
        return InFlightEvents;
    }
    public void addEvent(int partitionId, String stream,Object o){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.addEvents(partitionId,o);
        }
    }
    public void addEpoch(long offset, String stream){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.addEpoch(offset);
        }
    }
    public void addBatch(long markerId, String stream) {
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.addBatch(markerId);
        }
    }
    //GC after snapshot commit
    public void cleanEpoch(long offset, String stream){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.cleanEpoch(offset);
        }
    }
    public void cleanAll(String stream){
        for (InFlightLog inFlightLog:InFightLogForStream.get(stream).values()){
            inFlightLog.cleanAll();
        }
    }

    private class InFlightLog{
        //<EpochId,BatchEvent>
        private final ConcurrentHashMap<Long, BatchEvents> InFlightEvents = new ConcurrentHashMap<>();
        private long currentOffset;
        private final List<Integer> executorID;
        public InFlightLog(ArrayList<Integer> executorID){
            this.executorID = executorID;
            currentOffset = 0L;
            BatchEvents batchEvents = new BatchEvents(executorID, currentOffset);
            InFlightEvents.put(currentOffset,batchEvents);
        }
        public void addEvents(int partitionID, Object o){
            int executorId = executorID.get(partitionID);
            InFlightEvents.get(currentOffset).addEvent(o,executorId);
        }
        public void addBatch(long markerId) {
            InFlightEvents.get(currentOffset).addMarkerId(markerId);
        }
        public void addEpoch(long offset){
            currentOffset = offset;
            BatchEvents batchEvents = new BatchEvents(this.executorID, currentOffset);
            InFlightEvents.put(currentOffset, batchEvents);
        }
        public BatchEvents getInFlightEventsByOffset(long offset){
            return InFlightEvents.get(offset);
        }

        public void cleanEpoch(long offset){
            InFlightEvents.entrySet().removeIf(entry -> entry.getKey() < offset);
            LOG.info("Clean epoch before "+ offset);
        }
        public void cleanAll(){
            InFlightEvents.entrySet().removeIf(entry -> entry.getKey() == currentOffset);
            BatchEvents batchEvents = new BatchEvents(executorID, currentOffset);
            InFlightEvents.put(currentOffset, batchEvents);
        }
    }

    public class BatchEvents{
        //<markerId,<ExecutorId,events>>
        private final ConcurrentHashMap<Long,ConcurrentHashMap<Integer,List<Object>>> batchEvents = new ConcurrentHashMap<>();
        private long currentMarkerId;
        private final List<Integer> executorID;
        public BatchEvents(List<Integer> executorID,long currentMarkerId) {
            this.executorID = executorID;
            ConcurrentHashMap<Integer,List<Object>> eventsForExecutor = new ConcurrentHashMap<>();
            for (int id:executorID){
              List<Object> Events = new ArrayList<>();
              eventsForExecutor.put(id,Events);
            }
            this.currentMarkerId = currentMarkerId;
            batchEvents.put(currentMarkerId,eventsForExecutor);
        }
        public void addMarkerId(long markerId) {
            currentMarkerId = markerId;
            ConcurrentHashMap<Integer,List<Object>> eventsForExecutor = new ConcurrentHashMap<>();
            for (int id:executorID){
                List<Object> Events = new ArrayList<>();
                eventsForExecutor.put(id,Events);
            }
            batchEvents.put(currentMarkerId,eventsForExecutor);
        }

        public void addEvent(Object o, int executorId) {
            batchEvents.get(currentMarkerId).get(executorId).add(o);
        }
        public ConcurrentHashMap<Integer, Iterator<Object>> getBatchEvents(long markerId){
            ConcurrentHashMap<Integer, Iterator<Object>> iterators = new ConcurrentHashMap<>();
            for (int id : batchEvents.get(markerId).keySet()){
                Iterator<Object> iterator = batchEvents.get(markerId).get(id).iterator();
                iterators.put(id,iterator);
            }
            return iterators;
        }
        public List<Long> getMarkerId(){
            List<Long> ids = new ArrayList<>();
            for (Long markerId:batchEvents.keySet()){
                ids.add(markerId);
            }
            return ids;
        }
    }
}
