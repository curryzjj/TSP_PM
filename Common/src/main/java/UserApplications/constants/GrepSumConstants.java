package UserApplications.constants;

import System.constants.BaseConstants;

import static UserApplications.CONTROL.NUM_ITEMS;

public interface GrepSumConstants extends BaseConstants {
    String PREFIX="gstxn";
    interface Field extends BaseField {

    }
    interface Constant {
        int FREQUENCY_MICRO = 100;
        int VALUE_LEN = 32;// 32 bytes --> one cache line.
    }
    interface Conf extends BaseConf{
        int NUM_SEGMENTS = NUM_ITEMS;
        String Executor_Threads ="gs.txn.executor.threads";
    }
    interface Component extends BaseComponent {
        String EXECUTOR = "executor";
    }
}
