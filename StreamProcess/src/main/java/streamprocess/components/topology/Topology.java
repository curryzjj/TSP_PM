package streamprocess.components.topology;

import System.Platform.Platform;
import engine.Database;
import streamprocess.controller.input.InputStreamController;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Class to d_record a Topology description
 * Topology build (user side) -> (system side) build children link.
 * -> Topology Compile to GetAndUpdate execution graph -> link executor to each topology component.
 */
public class Topology implements Serializable {
    /**
     * <Operator ID, Operator>
     */
    //TODO initialize the DB
    public Database db;
    /**
    public TransactionTopology txnTopology;
    public SpinLock[] spinlock;
    public TableInitilizer tableinitilizer;
     **/
    private final LinkedHashMap<String, TopologyComponent> records;
    private TopologyComponent sink;//not sure where to use?
    private InputStreamController scheduler;
    private Platform p;

    public Topology() {
        records = new LinkedHashMap<>();
    }
    public Topology(Topology topology){
        records = new LinkedHashMap<>();
    }
    //Add/get element(spout/bolt) in topology
    public void addRecord(TopologyComponent rec) {
        records.put(rec.getId(),rec);
    }
    public TopologyComponent getRecord(String componentID) { return records.get(componentID); }
    public TopologyComponent getComponent(String componentId) { return records.get(componentId); }
    public LinkedHashMap<String, TopologyComponent> getRecords() { return records; }
    public void clean_executorInformation() {}
    //end

   //set sink
   public void setSink(TopologyComponent sink) {
       this.sink = sink;
   }
   //end

    //set input controller
    public InputStreamController getScheduler() {
        return scheduler;
    }
    public void setScheduler(InputStreamController sequentialScheduler) {
        scheduler = sequentialScheduler;
    }
    //end

    //Platform configure
    public void addMachine(Platform p) {
        this.p = p;
    }
    public Platform getPlatform() {
        return p;
    }
    public String getPrefix() { return ""; }
    //end
}
