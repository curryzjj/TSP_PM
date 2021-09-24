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
        String SINK_CLASS="%s.sink.class";
        String SPOUT_TEST_PATH = "%s.test.spout.path";
        String SPOUT_PATH = "%s.spout.path";
    }
}
