package streamprocess.components.topology;

import engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.operators.api.AbstractBolt;
import streamprocess.components.operators.api.AbstractSpout;
import streamprocess.components.operators.base.BaseSink;
import streamprocess.components.operators.executor.BasicBoltBatchExecutor;
import streamprocess.components.operators.executor.BasicSpoutBatchExecutor;
import streamprocess.components.operators.executor.IExecutor;
import streamprocess.controller.input.InputStreamController;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.execution.runtime.tuple.OutputFieldsDeclarer;
import streamprocess.execution.runtime.tuple.streaminfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static System.Constants.*;

/**
 * Builder pattern for Topology_components class
 */
public class TopologyBuilder {
    private static final Logger LOG= LoggerFactory.getLogger(TopologyBuilder.class);
    private final Topology topology;
    private final Set<String> idSet;
    public TopologyBuilder(){
        topology=new Topology();
        idSet=new HashSet<>();
    }
    /**
     * add spout to Brisk.topology
     *
     * @param id
     * @param s
     * @throws InvalidIDException
     */
    public TopologyBuilder setSpout(String id, AbstractSpout s,int numTasks) throws InvalidIDException{
        if(idSet.contains(id)){//id=spout,parser
            throw new InvalidIDException("operator ID already taken");
        }
        idSet.add(id);
        HashMap<String,streaminfo> output_streams;
        OutputFieldsDeclarer declarer=new OutputFieldsDeclarer();//implement after
        s.declareOutputFields(declarer);
        output_streams=declarer.getFieldsDeclaration();
        TopologyComponent topologyComponent=new MultiStreamComponent(id,spoutType,new BasicSpoutBatchExecutor(s),numTasks,null,output_streams,null);
        topology.addRecord(topologyComponent);
        return this;
    }
    public TopologyBuilder setBolt(String id, AbstractBolt b, int numTasks, Grouping... groups) throws InvalidIDException {
        return setBolt(boltType,id,b,numTasks,groups);
    }
    //some function to implement the setBolt
    /**
     * add bolt with shuffle Grouping to parent
     *
     * @param id
     * @param b
     * @param numTasks
     * @param groups   :multiple groups.
     * @throws InvalidIDException
     */
    private TopologyBuilder setBolt(char type, String id, AbstractBolt b, int numTasks, Grouping... groups) throws InvalidIDException {
        if (idSet.contains(id)) {
            throw new InvalidIDException("Operator ID already taken");
        }
        for(Grouping g:groups){
            if(!idSet.contains(g.getComponentId())){
                throw new InvalidIDException("parent ID"+g.getComponentId()+"dose not exist");
            }
        }
        idSet.add(id);
        HashMap<String,streaminfo> output_streams;
        OutputFieldsDeclarer declarer=new OutputFieldsDeclarer();
        b.declareOutputFields(declarer);
        output_streams=declarer.getFieldsDeclaration();
        final BasicBoltBatchExecutor executor=new BasicBoltBatchExecutor(b);
        setBolt(type,id,executor,output_streams,numTasks,groups);
        return this;
    }
    /**
     * @param type
     * @param id
     * @param b
     * @param output_streams
     * @param numTasks
     * @param groups
     * @return
     */
    private TopologyBuilder setBolt(char type, String id, IExecutor b, HashMap<String, streaminfo> output_streams, int numTasks, Grouping... groups) {
        ArrayList<String> input_streams=new ArrayList<>();
        for(Grouping g:groups){
            input_streams.add(g.getStreamID());
        }
        TopologyComponent _thisComponent;
        _thisComponent=new MultiStreamComponent(id,type,b,numTasks,input_streams,output_streams,groups);
        topology.addRecord(_thisComponent);
        for(Grouping g:groups){
            TopologyComponent parent=topology.getComponent(g.getComponentId());
            parent.setGrouping(id,g);
            parent.setChildren(_thisComponent,g);
            _thisComponent.setParents(parent,g);
        }
        if(type==sinkType){
            topology.setSink(_thisComponent);
        }
        return this;
    }
    //window
    //end
    //end
    public TopologyBuilder setSink(String id, AbstractBolt b, int numTasks, ShuffleGrouping groups) throws InvalidIDException {
        return setBolt(sinkType,id,b,numTasks,groups);
    }

    public void setGlobalScheduler(InputStreamController scheduler) {
        topology.setScheduler(scheduler);
    }

    public Topology createTopology() {
        if(topology.getScheduler()==null){
            LOG.info("JumboTuple scheduler is not set, use default Brisk.execution.runtime.tuple scheduler instead!");
            topology.setScheduler(new SequentialScheduler());
        }
        return topology;
    }

}
