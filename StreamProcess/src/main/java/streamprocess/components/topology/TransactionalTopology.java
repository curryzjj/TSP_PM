package streamprocess.components.topology;

import System.util.Configuration;
import engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.Initialize.TableInitilizer;
import utils.SpinLock;

public abstract class TransactionalTopology extends BasicTopology{
    private static final Logger LOG= LoggerFactory.getLogger(TransactionalTopology.class);
    public Database db;
    protected TransactionalTopology(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        InitializeDB();
    }
    protected abstract void InitializeDB();//decide which DB to use
    public abstract TableInitilizer createDB(SpinLock[] spinlock);
}
