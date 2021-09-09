package streamprocess.components.topology;

import java.io.Serializable;

/**
 * Helper class. SchemaRecord all necessary information about a spout/bolt.
 */
public class TopologyComponent implements Serializable {
    public char type;

    public String getId() { return ""; }
}
