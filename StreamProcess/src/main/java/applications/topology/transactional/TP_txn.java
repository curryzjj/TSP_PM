package applications.topology.transactional;

import System.util.Configuration;
import applications.bolts.transactional.tp.TPBolt_TStream_NoFT;
import applications.events.InputDataGenerator.ImplDataGenerator.TPDataGenerator;
import UserApplications.constants.TP_TxnConstants.Component;
import UserApplications.constants.TP_TxnConstants.Field;
import applications.bolts.transactional.tp.TPBolt_TStream_Snapshot;
import applications.bolts.transactional.tp.TPBolt_TStream_Wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TransactionalTopology;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.execution.Initialize.TableInitilizer;
import streamprocess.execution.Initialize.impl.TPInitializer;
import streamprocess.execution.runtime.tuple.Fields;
import utils.SpinLock;

import static UserApplications.CONTROL.*;
import static UserApplications.constants.TP_TxnConstants.Conf.Executor_Threads;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;
import static UserApplications.constants.TP_TxnConstants.PREFIX;
import static utils.PartitionHelper.setPartition_interval;

public class TP_txn extends TransactionalTopology {
    private static final Logger LOG= LoggerFactory.getLogger(TP_txn.class);
    public TP_txn(String topologyName, Configuration config) {
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
            spout.setFields(new Fields(Field.TEXT));
            spout.setInputDataGenerator(new TPDataGenerator());
            builder.setSpout(Component.SPOUT,spout,spoutThreads);
//            builder.setBolt(Component.DISPATCHER,
//                    new DispatcherBolt(),
//                    1,
//                    new ShuffleGrouping(Component.SPOUT));
            if(enable_snapshot){
                builder.setBolt(Component.EXECUTOR,
                        new TPBolt_TStream_Snapshot(0),
                        config.getInt(Executor_Threads,1),
                        new ShuffleGrouping(Component.SPOUT)
                );
            }else if(enable_wal){
                builder.setBolt(Component.EXECUTOR,
                        new TPBolt_TStream_Wal(0),
                        config.getInt(Executor_Threads,1),
                        new ShuffleGrouping(Component.SPOUT)
                );
            }else {
                builder.setBolt(Component.EXECUTOR,
                        new TPBolt_TStream_NoFT(0),
                        config.getInt(Executor_Threads,1),
                        new ShuffleGrouping(Component.SPOUT)
                );
            }
            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.EXECUTOR)
            );
        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology(db, this);
    }

    @Override
    protected Logger getLogger() {
        return null;
    }

    @Override
    protected String getConfigPrefix() {
        return PREFIX;
    }

    @Override
    public TableInitilizer createDB(SpinLock[] spinlock) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        setPartition_interval((int) (Math.ceil(NUM_SEGMENTS / (double) partition_num)), partition_num);
        TableInitilizer ini = new TPInitializer(db, scale_factor, theta, partition_num, config);
        ini.creates_Table(config);
        return ini;
    }
}
