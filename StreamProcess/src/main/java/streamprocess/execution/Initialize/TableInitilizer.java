package streamprocess.execution.Initialize;

import System.tools.FastZipfGenerator;
import System.util.Configuration;
import engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyContext;

import java.util.List;
import java.util.SplittableRandom;

public abstract class TableInitilizer {
    private static final Logger LOG= LoggerFactory.getLogger(TableInitilizer.class);
    protected final Database db;
    protected final double scale_factor;
    protected final double theta;
    protected final int tthread;
    protected final Configuration config;
    int floor_interval;
    //used for partition.
    protected long[] p_bid;
    protected int number_partitions;
    protected boolean[] multi_partion_decision;

    protected transient FastZipfGenerator p_generator;
    SplittableRandom rnd = new SplittableRandom(1234);

    protected TableInitilizer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        this.db = db;
        this.scale_factor = scale_factor;
        this.theta = theta;
        this.tthread = tthread;
        this.config = config;
    }

    public abstract void creates_Table(Configuration config);

    public abstract void loadDB(int thread_id, TopologyContext context);
    public abstract void reloadDB(List<Integer> rangeId);
}
