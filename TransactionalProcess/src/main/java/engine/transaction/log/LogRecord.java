package engine.transaction.log;

import java.io.Serializable;

public class LogRecord implements Serializable {
    private long timestamp;
    private String operationType;
    private String key;
    private String tableName;
    public LogRecord(String key,String tableName){
        this.key=key;
        this.tableName=tableName;
    }
}
