package streamprocess.execution.Initialize;

import System.util.Configuration;
import engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyContext;

import java.util.List;

public abstract class TableInitilizer {
    private static final Logger LOG= LoggerFactory.getLogger(TableInitilizer.class);
    protected final Database db;
    protected final double scale_factor;
    protected final double theta;
    protected final int partition_id;
    protected final Configuration config;

    protected TableInitilizer(Database db, double scale_factor, double theta, int partition_id, Configuration config) {
        this.db = db;
        this.scale_factor = scale_factor;
        this.theta = theta;
        this.partition_id = partition_id;
        this.config = config;
    }

    public abstract void creates_Table(Configuration config);

    public abstract void loadDB(int thread_id, TopologyContext context);
    public abstract void reloadDB(List<Integer> rangeId);
}
