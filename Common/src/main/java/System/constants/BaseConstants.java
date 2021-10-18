package System.constants;
import static System.Constants.DEFAULT_STREAM_ID;
public interface BaseConstants {
    String BASE_PREFIX = "compatibility";
    interface BaseField {
        String SYSTEMTIMESTAMP = "systemtimestamp";
        String MSG_ID = "systemmsgID";
        String TEXT = "text";
    }
    interface BaseStream {
        String DEFAULT = DEFAULT_STREAM_ID;
    }
    interface BaseComponent {
        String PARSER = "parser";
        String SPOUT = "spout";
        String SINK = "sink";
        String FORWARD = "forward";
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
    }
}
