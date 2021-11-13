package System.constants;
public interface BaseConstants {
    String BASE_PREFIX = "compatibility";
    interface BaseField {
        String SYSTEMTIMESTAMP = "systemtimestamp";
        String MSG_ID = "systemmsgID";
        String TEXT = "text";
    }
    interface BaseStream {
        String DEFAULT = "default";
        //Stream process constants
        String DEFAULT_STREAM_ID = "default";
        char sinkType = 's';
        char spoutType = 'p';
        char boltType = 'b';
        char virtualType = 'v';
    }
    interface BaseComponent {
        String PARSER = "parser";
        String SPOUT = "spout";
        String SINK = "sink";
    }
    interface BaseConf{
        String SPOUT_CLASS="%s.spout.class";
        String SPOUT_TEST_PATH = "%s.test.spout.path";
        String SPOUT_PATH = "%s.spout.path";
        String SPOUT_PARSER = "%s.spout.parser";
        String SINK_CLASS="%s.sink.class";
        String SINK_FORMATTER = "%s.sink.formatter";
        String SPOUT_THREADS ="%s.spout.threads" ;
        String SINK_THREADS = "%s.sink.threads";
        String RECORD_NUM_TEST="%s.test.recordnum";
        String RECORD_NUM="%s.recordnum";
        String ZIPSKEW_TEST="%s.test.zipskew";
        String ZIPSKEW_NUM="%s.zipskew";

    }
    interface CCOptions{
        int CCOption_LOCK = 0;
        int CCOption_OrderLOCK = 1;
        int CCOption_LWM = 2;
        int CCOption_TStream = 3;
        int CCOption_SStore = 4;
        int CCOption_OTS = 5;//ordered timestamp
    }
    interface DBOptions{
        int In_Memory=0;
        int RocksDB=1;
    }
}
