package applications.topology.transactional;

import System.util.Configuration;
import UserApplications.constants.TP_TxnConstants.Component;
import UserApplications.constants.TP_TxnConstants.Field;
import applications.DataTypes.util.LRTopologyControl;
import applications.DataTypes.util.SegmentIdentifier;
import applications.bolts.transactional.tp.DispatcherBolt;
import applications.bolts.transactional.tp.TPBolt_TStream;
import engine.ImplDatabase.InMemeoryDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.FieldsGrouping;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TransactionalTopology;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.execution.Initialize.TableInitilizer;
import streamprocess.execution.Initialize.impl.TPInitializer;
import streamprocess.execution.runtime.tuple.Fields;
import utils.SpinLock;

import static UserApplications.constants.TP_TxnConstants.Conf.Executor_Threads;
import static UserApplications.constants.TP_TxnConstants.PREFIX;
import static UserApplications.constants.TP_TxnConstants.Stream.POSITION_REPORTS_STREAM_ID;

public class TP_txn extends TransactionalTopology {
    private static final Logger LOG= LoggerFactory.getLogger(TP_txn.class);
    protected TP_txn(String topologyName, Configuration config) {
        super(topologyName, config);
    }
    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT,spout,sinkThreads);
            builder.setBolt(Component.DISPATCHER,
                    new DispatcherBolt(),
                    1,
                    new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.EXECUTOR,
                    new TPBolt_TStream(0),
                    config.getInt(Executor_Threads,2),
                    new FieldsGrouping(Component.DISPATCHER,POSITION_REPORTS_STREAM_ID, SegmentIdentifier.getSchema())
                    );
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
        int tthread = config.getInt("tthread");
        TableInitilizer ini = new TPInitializer(db, scale_factor, theta, tthread, config);
        ini.creates_Table(config);
        return ini;
    }
}
