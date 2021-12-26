package applications.topology.transactional;


import System.util.Configuration;
import UserApplications.constants.StreamLedgerConstants;
import applications.bolts.transactional.sl.SLBolt_TStream_NoFT;
import applications.bolts.transactional.sl.SLBolt_TStream_Snapshot;
import applications.bolts.transactional.sl.SLBolt_TStream_Wal;
import applications.events.InputDataGenerator.ImplDataGenerator.SLDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.InvalidIDException;
import streamprocess.components.grouping.ShuffleGrouping;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TransactionalTopology;
import streamprocess.controller.input.scheduler.SequentialScheduler;
import streamprocess.execution.Initialize.TableInitilizer;
import streamprocess.execution.Initialize.impl.SLInitializer;
import streamprocess.execution.runtime.tuple.Fields;
import utils.SpinLock;
import System.constants.BaseConstants.BaseConf;

import static UserApplications.CONTROL.*;
import static UserApplications.constants.OnlineBidingSystemConstants.Conf.Executor_Threads;
import static UserApplications.constants.StreamLedgerConstants.PREFIX;
import static utils.PartitionHelper.setPartition_interval;;

public class SL_txn extends TransactionalTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SL_txn.class);
    public SL_txn(String topologyName, Configuration config) {
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
            spout.setFields(new Fields(StreamLedgerConstants.Field.TEXT));
            spout.setInputDataGenerator(new SLDataGenerator());
            builder.setSpout(StreamLedgerConstants.Component.SPOUT,spout,spoutThreads);
            if(enable_snapshot){
                builder.setBolt(StreamLedgerConstants.Component.EXECUTOR,
                        new SLBolt_TStream_Snapshot(0),
                        config.getInt(Executor_Threads),
                        new ShuffleGrouping(StreamLedgerConstants.Component.SPOUT));
            }else if(enable_wal){
                builder.setBolt(StreamLedgerConstants.Component.EXECUTOR,
                        new SLBolt_TStream_Wal(0),
                        config.getInt(Executor_Threads),
                        new ShuffleGrouping(StreamLedgerConstants.Component.SPOUT));
            }else{
                builder.setBolt(StreamLedgerConstants.Component.EXECUTOR,
                        new SLBolt_TStream_NoFT(0),
                        config.getInt(Executor_Threads),
                        new ShuffleGrouping(StreamLedgerConstants.Component.SPOUT));
            }
            builder.setSink(StreamLedgerConstants.Component.SINK,sink,sinkThreads,new ShuffleGrouping(StreamLedgerConstants.Component.EXECUTOR));
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
        setPartition_interval((int) (Math.ceil(NUM_ITEMS / (double) partition_num)), partition_num);
        TableInitilizer ini = new SLInitializer(db, scale_factor, theta, partition_num, config);
        ini.creates_Table(config);
        return ini;
    }
}