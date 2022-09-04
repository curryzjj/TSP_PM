package engine.log;

import engine.table.tableRecords.TableRecord;

import java.io.IOException;
import java.io.Serializable;

public class LogRecord implements Serializable {
    private static final long serialVersionUID = -9072621686098189801L;
    private long timestamp;
    private String operationType;
    private String key;
    private String tableName;
    //used in the recovery
    private String[] values;
    /* used to redo transactions in the failure recovery phase */
    private TableRecord updateTableRecord;
    /* used to undo transaction in the transaction abort phase  */
    private TableRecord copyTableRecord;
    protected final String split_exp = ";";
    public LogRecord(String key,String tableName){
        this.key=key;
        this.tableName=tableName;
    }
    //used in recovery
    public LogRecord(String recoveryString){
        String[] split = recoveryString.split(";");
        this.tableName=split[1];
        this.key=split[0];
        this.timestamp = Long.parseLong(split[2]);
        this.values = split[3].split(",");
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTableName() {
        return tableName;
    }

    public String getKey() {
        return key;
    }

    public String[] getValues() {
        return values;
    }

    public TableRecord getUpdateTableRecord() {
        return updateTableRecord;
    }

    public void setUpdateTableRecord(TableRecord updateTableRecord) {
        this.updateTableRecord = updateTableRecord;
    }

    public TableRecord getCopyTableRecord() {
        return copyTableRecord;
    }

    public void setCopyTableRecord(TableRecord copyTableRecord) {
        try {
            this.copyTableRecord = copyTableRecord.cloneTableRecord();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public String toSerializableString() throws IOException {
        this.updateTableRecord.clean_map();
        StringBuilder sb = new StringBuilder();
        sb.append(this.key);//key
        sb.append(split_exp);
        sb.append(this.tableName);
        sb.append(split_exp);
        sb.append(this.updateTableRecord.versions.lastKey().toString());
        sb.append(split_exp);
        sb.append(this.updateTableRecord.record_.toString());
        return sb.toString();
    }
}
