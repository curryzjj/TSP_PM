package UserApplications.constants;

import System.constants.BaseConstants;

import static UserApplications.CONTROL.NUM_ITEMS;

public interface GrepSumConstants extends BaseConstants {
    String PREFIX="gstxn";
    interface Field extends BaseField {

    }
    interface Constant {
        int VALUE_LEN = 32;// 32 bytes --> one cache line.
        int MAX_VALUE = 100;
    }
    interface Conf extends BaseConf{
        int NUM_SEGMENTS = NUM_ITEMS;
        String Executor_Threads ="executor.threads";
    }
    interface Component extends BaseComponent {
        String EXECUTOR = "executor";
    }
}
