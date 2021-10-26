package applications.topology.transactional;

import System.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TransactionalTopology;
import streamprocess.execution.Initialize.TableInitilizer;
import utils.SpinLock;

import static UserApplications.constants.TP_TxnConstants.PREFIX;

public class TP_txn extends TransactionalTopology {
    private static final Logger LOG= LoggerFactory.getLogger(TP_txn.class);
    protected TP_txn(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    @Override
    public Topology buildTopology() {
        return null;
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
    protected void InitializeDB() {

    }

    @Override
    public TableInitilizer createDB(SpinLock[] spinlock) {
        return null;
    }
}
