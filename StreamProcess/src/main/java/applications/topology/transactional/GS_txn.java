package applications.topology.transactional;

import System.util.Configuration;
import UserApplications.constants.GrepSumConstants;
import UserApplications.constants.GrepSumConstants.Component;
import UserApplications.constants.TP_TxnConstants;
import applications.bolts.transactional.gs.GSBolt_TStream_NoFT;
import applications.bolts.transactional.gs.GSBolt_TStream_Snapshot;
import applications.bolts.transactional.gs.GSBolt_TStream_Wal;
import applications.events.InputDataGenerator.ImplDataGenerator.GSDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TransactionalTopology;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.execution.Initialize.TableInitilizer;
import streamprocess.execution.Initialize.impl.GSInitializer;
import streamprocess.execution.runtime.tuple.Fields;
import utils.SpinLock;

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
            spout.setInputDataGenerator(new GSDataGenerator());
            builder.setSpout(Component.SPOUT,spout,sinkThreads);
            if (enable_snapshot){
                builder.setBolt(Component.EXECUTOR,
                        new GSBolt_TStream_Snapshot(0),
                        config.getInt(Executor_Threads),
                        new ShuffleGrouping(Component.SPOUT));
            }else if(enable_wal){
                builder.setBolt(Component.EXECUTOR,
                        new GSBolt_TStream_Wal(0),
                        config.getInt(Executor_Threads),
                        new ShuffleGrouping(Component.SPOUT));
            }else {
                builder.setBolt(Component.EXECUTOR,
                        new GSBolt_TStream_NoFT(0),
                        config.getInt(Executor_Threads),
                        new ShuffleGrouping(Component.SPOUT));
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
    public TableInitilizer createDB(SpinLock[] spinlock) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt(Executor_Threads,1);
        setPartition_interval((int) (Math.ceil(NUM_ITEMS / (double) tthread)), tthread);
        TableInitilizer ini = new GSInitializer(db, scale_factor, theta, tthread, config);
        ini.creates_Table(config);
        return ini;
    }
}
