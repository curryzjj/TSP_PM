package UserApplications.constants;

import System.constants.BaseConstants;

import static UserApplications.CONTROL.NUM_ACCESSES;

public interface OnlineBidingSystemConstants extends BaseConstants {
    String PREFIX = "obtxn";

    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
        String FLAG = "flag";
    }

    interface Conf extends BaseConf {
        String Executor_Threads ="executor.threads";
    }

    interface Component extends BaseComponent {
        String EXECUTOR = "executor";
    }

    interface Constant {

        int NUM_ACCESSES_PER_BUY = 1;// each time only bid one item..
        int NUM_ACCESSES_PER_TOP = NUM_ACCESSES;// each time top up 20 items.
        int NUM_ACCESSES_PER_ALERT = NUM_ACCESSES;// each time alert 20 items.

        long MAX_BUY_Transfer = 20;
        long MAX_TOP_UP = 20;
        int MAX_Price = 100;

    }
}
