package streamprocess.controller.output.Determinant;

import engine.table.tableRecords.SchemaRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Add to the operation_chain when recovery
 */
public class OutsideDeterminant implements Serializable {
    private static final long serialVersionUID = 1697109885782459412L;
    public String outSideEvent;
    public List<Integer> targetPartitionIds = new ArrayList<>();
    public HashMap<String, SchemaRecord> ackValues = new HashMap<>();
    public void setOutSideEvent(String outSideEvent) {
        this.outSideEvent = outSideEvent;
    }
    public void setTargetPartitionId(int targetId) {
        if (this.targetPartitionIds.contains(targetId)){
            return;
        }
        this.targetPartitionIds.add(targetId);
    }
    public void setAckValues(String key, SchemaRecord schemaRecord) {
        this.ackValues.put(key, schemaRecord);
    }
}
