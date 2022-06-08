package streamprocess.controller.output.Determinant;

import engine.table.tableRecords.SchemaRecord;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Return the determinant result when recovery
 */
public class InsideDeterminant implements Serializable {
    private static final long serialVersionUID = -236979220636840984L;
    public long input;
    public boolean isAbort;
    //<Key,SchemaRecord>
    public HashMap<String,SchemaRecord> ackValues;
    public InsideDeterminant(long input){
        this.input = input;
        this.ackValues = new HashMap<>();
        this.isAbort = false;
    }

    public void setAbort(boolean abort) {
        isAbort = abort;
    }

    public void setAckValues(String key, SchemaRecord schemaRecord) {
        this.ackValues.put(key, schemaRecord);
    }
}
