package System;

import System.util.OsUtils;

public interface Constants {
    //System constants
    String Mac_Project_Path=System.getProperty("user.home").concat("/hair-loss/TSP_PM");
    String Node22_Project_Path = System.getProperty("user.home").concat("/TSP_PM");
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
