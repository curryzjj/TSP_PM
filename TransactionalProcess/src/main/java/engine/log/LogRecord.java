package engine.log;

import engine.table.tableRecords.TableRecord;

import java.io.Serializable;

public class LogRecord implements Serializable {
    private static final long serialVersionUID = -9072621686098189801L;
    private long timestamp;
    private String operationType;
    private String key;
    private String tableName;
    /* used to redo transactions in the failure recovery phase */
    private TableRecord updateTableRecord;
    /* used to undo transaction in the transaction abort phase  */
    private TableRecord copyTableRecord;
    public LogRecord(String key,String tableName){
        this.key=key;
        this.tableName=tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getKey() {
        return key;
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
        this.copyTableRecord = copyTableRecord;
    }
}
