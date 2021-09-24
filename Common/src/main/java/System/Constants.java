package System;

public interface Constants {
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
