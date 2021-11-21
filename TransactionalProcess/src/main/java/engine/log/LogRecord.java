package engine.log;

import engine.table.tableRecords.TableRecord;

import java.io.Serializable;

public class LogRecord implements Serializable {
    private static final long serialVersionUID = -9072621686098189801L;
    private long timestamp;
    private String operationType;
    private String key;
    private String tableName;
    private TableRecord updateTableRecord;
    public LogRecord(String key,String tableName){
        this.key=key;
        this.tableName=tableName;
    }

    public TableRecord getUpdateTableRecord() {
        return updateTableRecord;
    }

    public void setUpdateTableRecord(TableRecord updateTableRecord) {
        this.updateTableRecord = updateTableRecord;
    }
}
