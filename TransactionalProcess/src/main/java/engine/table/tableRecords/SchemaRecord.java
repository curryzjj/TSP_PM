package engine.table.tableRecords;

import engine.table.RecordSchema;
import engine.table.RowID;
import engine.table.datatype.DataBox;
import utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A wrapper class for an individual d_record. Simply stores a list of DataBoxes.
 */
public class SchemaRecord implements Serializable {
    private RowID id;
    private volatile List<DataBox> values;
    private final DataBox single_value;//only used by TSTREAM.
    public SchemaRecord(DataBox value) {
        this.single_value = value;
    }
    public SchemaRecord(List<DataBox> values) {
        this.values = values;
        single_value = null;
    }
    public SchemaRecord(SchemaRecord _record_ptr){
        this.id = _record_ptr.id;
        this.values= Utils.memcpy(_record_ptr.values);
        single_value=null;
    }
    public List<DataBox> getValues() {
        return this.values;
    }
    public DataBox getValue() {
        return this.single_value;
    }
    public RowID getId() {
        return id;
    }
    public void setID(RowID ID) {
        this.id = ID;
    }
    /**
     * Assume the primary key is always a String, and is always the first field.
     *
     * @return
     */
    public String GetPrimaryKey() {
        return values.get(0).getString();
    }
    public void updateValues(List<DataBox> values) {
        this.values = Utils.memcpy(values);
    }
    public void clean() {
        values = null;
    }
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SchemaRecord)) {
            return false;
        }
        SchemaRecord otherRecord = (SchemaRecord) other;
        if (values.size() != otherRecord.values.size()) {
            return false;
        }
        for (int i = 0; i < values.size(); i++) {
            if (!(values.get(i).equals(otherRecord.values.get(i)))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        if (values != null) {
            StringBuilder s = new StringBuilder();
            for (DataBox d : values) {
                s.append(d.toString());
                s.append(",");
            }
            return s.toString();
        } else if (single_value != null) {
            return single_value.toString();
        } else
            return getId().toString() + " have no value_list";
    }
}
