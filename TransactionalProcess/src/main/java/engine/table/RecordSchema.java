package engine.table;

import engine.table.datatype.DataBox;

import java.io.Serializable;
import java.util.List;

/**
 * The RecordSchema of a particular table.
 * <p>
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a d_record conforming to this schema
 */
public class RecordSchema implements Serializable {
    private final int secondary_num;
    private List<String> fields;
    private List<DataBox> fieldTypes;
    private int size;
    public RecordSchema(List<String> fields, List<DataBox> fieldTypes){
        assert (fields.size() == fieldTypes.size());
        this.fields = fields;
        this.fieldTypes = fieldTypes;
        this.size=0;
        for (DataBox dt : fieldTypes) {
            this.size += dt.getSize();
        }
        secondary_num = fields.size();
    }

    public List<DataBox> getFieldTypes() {
        return fieldTypes;
    }
}
