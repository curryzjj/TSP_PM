package applications.topology.transactional;

import System.util.Configuration;
import UserApplications.constants.GrepSumConstants;
import UserApplications.constants.GrepSumConstants.Component;
import applications.bolts.transactional.gs.*;
import applications.events.InputDataStore.ImplDataStore.GSInputStore;
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
import streamprocess.execution.Initialize.impl.GSInitializer;
import streamprocess.execution.runtime.tuple.Fields;
import utils.SpinLock;

import java.io.IOException;

import static UserApplications.CONTROL.*;
import static UserApplications.constants.GrepSumConstants.Conf.Executor_Threads;
import static UserApplications.constants.GrepSumConstants.PREFIX;
import static utils.PartitionHelper.setPartition_interval;


public class GS_txn extends TransactionalTopology {
    private static final Logger LOG = LoggerFactory.getLogger(GS_txn.class);
    public GS_txn(String topologyName, Configuration config) {
        super(topologyName, config);
        config.put("tthread",config.getInt(Executor_Threads,1));
    }

    @Override
    public void initialize() {
        super.initialize();
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(GrepSumConstants.Field.TEXT));
            spout.setInputStore(new GSInputStore());
            builder.setSpout(Component.SPOUT,spout,spoutThreads);
            Grouping grouping;
            if (enable_key_based) {
                grouping = new KeyBasedGrouping(Component.SPOUT);
            } else {
                grouping = new ShuffleGrouping(Component.SPOUT);
            }
            if (enable_checkpoint){
                if (conventional) {
                    builder.setBolt(Component.EXECUTOR,
                            new GSBolt_SStore_Global(0),
                            config.getInt(Executor_Threads),
                            grouping);
                } else {
                    builder.setBolt(Component.EXECUTOR,
                            new GSBolt_TStream_ISC(0),
                            config.getInt(Executor_Threads),
                            grouping);
                }
            }else if(enable_wal){
                builder.setBolt(Component.EXECUTOR,
                        new GSBolt_TStream_WSC(0),
                        config.getInt(Executor_Threads),
                        grouping);
            }else if(enable_clr){
                if (conventional) {
                    builder.setBolt(Component.EXECUTOR,
                            new GSBolt_TStream_Local(0),
                            config.getInt(Executor_Threads),
                            grouping);
                } else {
                    builder.setBolt(Component.EXECUTOR,
                            new GSBolt_TStream_CLR(0),
                            config.getInt(Executor_Threads),
                            grouping);
                }
            }else {
                builder.setBolt(Component.EXECUTOR,
                        new GSBolt_TStream_NoFT(0),
                        config.getInt(Executor_Threads),
                        grouping);
            }
            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.EXECUTOR));
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
        TableInitilizer ini = new GSInitializer(db, scale_factor, theta, PARTITION_NUM, config);
        ini.creates_Table(config);
        return ini;
    }
}
