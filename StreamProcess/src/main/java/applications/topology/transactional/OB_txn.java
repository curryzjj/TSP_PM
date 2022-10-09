package applications.topology.transactional;

import System.util.Configuration;
import UserApplications.constants.GrepSumConstants;
import UserApplications.constants.OnlineBidingSystemConstants;
import applications.bolts.transactional.ob.*;
import applications.events.InputDataStore.ImplDataStore.OBInputStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.Grouping;
import streamprocess.components.grouping.KeyBasedGrouping;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TransactionalTopology;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.execution.Initialize.TableInitilizer;
import streamprocess.execution.Initialize.impl.OBInitiallizer;
import streamprocess.execution.runtime.tuple.Fields;
import utils.SpinLock;

import java.io.IOException;

import static UserApplications.CONTROL.*;
import static UserApplications.constants.OnlineBidingSystemConstants.Conf.Executor_Threads;
import static UserApplications.constants.OnlineBidingSystemConstants.PREFIX;
import static utils.PartitionHelper.setPartition_interval;


public class OB_txn extends TransactionalTopology {
    private static final Logger LOG = LoggerFactory.getLogger(OB_txn.class);
    public OB_txn(String topologyName, Configuration config) {
        super(topologyName, config);
        config.put("tthread",config.getInt(Executor_Threads,1));
    }

    @Override
    public void initialize() {
        super.initialize();
    }
    @Override
    public Topology buildTopology() {
       try{
           spout.setFields(new Fields(OnlineBidingSystemConstants.Field.TEXT));
           spout.setInputStore(new OBInputStore());
           builder.setSpout(OnlineBidingSystemConstants.Component.SPOUT,spout,spoutThreads);
           Grouping grouping;
           if (enable_key_based) {
               grouping = new KeyBasedGrouping(GrepSumConstants.Component.SPOUT);
           } else {
               grouping = new ShuffleGrouping(GrepSumConstants.Component.SPOUT);
           }
           if(enable_checkpoint){
               if (conventional) {
                   builder.setBolt(OnlineBidingSystemConstants.Component.EXECUTOR,
                           new OBBolt_SStore_Global(0),
                           config.getInt(Executor_Threads),
                           new KeyBasedGrouping(GrepSumConstants.Component.SPOUT));
               } else {
                   builder.setBolt(OnlineBidingSystemConstants.Component.EXECUTOR,
                           new OBBolt_TStream_ISC(0),
                           config.getInt(Executor_Threads),
                           grouping);
               }
           }else if(enable_wal){
               builder.setBolt(OnlineBidingSystemConstants.Component.EXECUTOR,
                       new OBBolt_TStream_WSC(0),
                       config.getInt(Executor_Threads),
                       grouping);
           }else if(enable_clr){
               if (conventional) {
                   builder.setBolt(OnlineBidingSystemConstants.Component.EXECUTOR,
                           new OBBolt_TStream_Local(0),
                           config.getInt(Executor_Threads),
                           grouping);
               } else {
                   builder.setBolt(OnlineBidingSystemConstants.Component.EXECUTOR,
                           new OBBolt_TStream_CLR(0),
                           config.getInt(Executor_Threads),
                           grouping);
               }
           }else{
               builder.setBolt(OnlineBidingSystemConstants.Component.EXECUTOR,
                       new OBBolt_TStream_NoFT(0),
                       config.getInt(Executor_Threads),
                       grouping);
           }
           builder.setSink(OnlineBidingSystemConstants.Component.SINK,sink,sinkThreads,new ShuffleGrouping(OnlineBidingSystemConstants.Component.EXECUTOR));
       } catch (InvalidIDException e) {
           e.printStackTrace();
       }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology(db, this);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected String getConfigPrefix() {
        return PREFIX;
    }

    @Override
    public TableInitilizer createDB(SpinLock[] spinlock) throws IOException {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        setPartition_interval((int) (Math.ceil(NUM_ITEMS / (double) PARTITION_NUM)), PARTITION_NUM);
        TableInitilizer ini = new OBInitiallizer(db, scale_factor, theta, PARTITION_NUM, config);
        ini.creates_Table(config);
        return ini;
    }
}
