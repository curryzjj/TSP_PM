package UserApplications.constants;

import System.constants.BaseConstants;

import static UserApplications.CONTROL.NUM_ITEMS;

public interface TP_TxnConstants extends BaseConstants {
    String PREFIX="tptxn";
    interface Field extends BaseField {
        String TIMESTAMP = "timestamp";
        String VEHICLE_ID = "vehicleId";
        String SPEED = "speed";
        String EXPRESSWAY = "expressway";
        String LANE = "lane";
        String DIRECTION = "direction";
        String SEGMENT = "segment";
        String POSITION = "position";
    }
    interface Conf extends BaseConf{
        int NUM_SEGMENTS = NUM_ITEMS;
        String Executor_Threads ="executor.threads";
    }
    interface Component extends BaseComponent {
        String DISPATCHER = "DISPATCHER";
        String EXECUTOR = "executor";
    }
    interface Stream extends BaseStream{
        String POSITION_REPORTS_STREAM_ID = "pr";
    }
}