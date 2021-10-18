package applications.sink.formatter;

import System.util.Configuration;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.runtime.tuple.Tuple;

public abstract class Formatter {
    TopologyContext context;

    public void initialize(Configuration config, TopologyContext context) {
        this.context = context;
    }

    public abstract String format(Tuple tuple);
}
