package streamprocess.faulttolerance.logger;

import java.io.Serializable;

public class LogRecord implements Serializable {
    private long timestamp;
    private String operationType;
    private String key;
    private String tableName;
    public LogRecord(long timestamp,String operationType,String key,String tableName){

    }
}