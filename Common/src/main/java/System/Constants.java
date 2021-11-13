package System;

import System.util.OsUtils;

public interface Constants {
    //System constants
    String Project_Path=System.getProperty("user.home")
            + OsUtils.OS_wrapper("hair-loss") + OsUtils.OS_wrapper("TSP_PM_Result");
    String STAT_Path = System.getProperty("user.home")
            + OsUtils.OS_wrapper("TSP_PM") + OsUtils.OS_wrapper("STAT");
    String Mac_Data_Path=System.getProperty("user.home").concat("/hair-loss/app/benchmarks/");
    String Node22_Data_Path=System.getProperty("user.home").concat("/app/benchmarks/");
    //PM constants
    String Default_Heap_Path="/mnt/persist-memory/pmem0/jjzhao";
    String Default_Heap_Name="/default";
    //Event constant
    String Default_split_expression="/t";
    String Default_null_expression=" ";
    //DB constants
    String Mac_RocksDB_Path=System.getProperty("user.home").concat("/hair-loss/app/RocksDB/");
    String Node22_RocksDB_Path=System.getProperty("user.home").concat("/app/RocksDB/");
}
