package streamprocess.controller.output.Determinant;

import engine.table.tableRecords.SchemaRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Return the determinant result when recovery
 */
public class InsideDeterminant implements Serializable {
    private static final long serialVersionUID = -236979220636840984L;
    public long input;
    public boolean isAbort;
    public int partitionId;
    //<Key,SchemaRecord>
    public HashMap<String, SchemaRecord> ackValues;
    public HashMap<String, List<SchemaRecord>> ackValueList;
    public InsideDeterminant(long input, int partitionId){
        this.input = input;
        this.ackValues = new HashMap<>();
        this.ackValueList = new HashMap<>();
        this.isAbort = false;
        this.partitionId = partitionId;
    }

    public void setAbort(boolean abort) {
        isAbort = abort;
    }

    public void setAckValues(String key, SchemaRecord schemaRecord) {
        this.ackValues.put(key, schemaRecord);
    }

}
