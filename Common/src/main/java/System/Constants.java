package System;

import System.util.OsUtils;

public interface Constants {
    String Project_Path=System.getProperty("user.home")
//            + (OsUtils.isMac() ? "" : OsUtils.OS_wrapper("Documents"))
            + OsUtils.OS_wrapper("hair-loss") + OsUtils.OS_wrapper("TSP_PM_Result");
    String STAT_Path = System.getProperty("user.home")
//            + (OsUtils.isMac() ? "" : OsUtils.OS_wrapper("Documents"))
            + OsUtils.OS_wrapper("TSP_PM") + OsUtils.OS_wrapper("STAT");
    //Stream process constants
    String DEFAULT_STREAM_ID = "default";
    char sinkType = 's';
    char spoutType = 'p';
    char boltType = 'b';
    char virtualType = 'v';
    //PM constants
    String Default_Heap_Path="/mnt/persist-memory/pmem0/jjzhao";
    String Default_Heap_Name="/default";
    //Event constant
    String Default_split_expression="/t";
    String Default_null_expression=" ";
}
