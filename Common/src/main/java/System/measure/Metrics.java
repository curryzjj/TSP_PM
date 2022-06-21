package System.measure;

import System.FileSystem.ImplFS.LocalFileSystem;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Metrics {
    static class Performance {
        public static ConcurrentHashMap<Integer,Double> AvgThroughput;
        public static ConcurrentHashMap<Integer,DescriptiveStatistics> Latency;
        public static ConcurrentHashMap<Integer, List<Double>> latency_map;
        public static ConcurrentHashMap<Integer,List<Double>> throughput_map;
        public static void Initialize() {
            AvgThroughput = new ConcurrentHashMap<>();
            Latency = new ConcurrentHashMap<>();
            latency_map = new ConcurrentHashMap<>();
            throughput_map = new ConcurrentHashMap<>();
        }
    }
    //Runtime Breakdown
    static class Runtime_Breakdown{
        //Wait Time
        public static DescriptiveStatistics wait_time = new DescriptiveStatistics();
        //Input_Store
        public static DescriptiveStatistics input_store_time = new DescriptiveStatistics();
        public static long input_store_begin_time;
        //Upstream backup
        public static DescriptiveStatistics[] upstream_backup_time;
        public static long[] upstream_backup_begin;
        public static double[] upstream_backup_acc;
        //Snapshot
        public static long Snapshot_begin_time;
        public static final DescriptiveStatistics Snapshot_time = new DescriptiveStatistics();
        //HelpLog
        // 1.WAL
        public static long  WAL_begin_time;
        public static final DescriptiveStatistics Wal_time = new DescriptiveStatistics();
        // 2.CLR
        public static DescriptiveStatistics[] Help_Log;
        public static long[] Help_Log_begin;
        public static double[] Help_Log_backup_acc;
        //Txn_time
        public static DescriptiveStatistics[] transaction_run_time;
        public static long[] transaction_begin_time;
        //Post_time
        public static DescriptiveStatistics[] transaction_post_time;
        public static long[] transaction_post_begin_time;
        //Construction Time
        public static DescriptiveStatistics[] transaction_construction_time;
        public static long[] transaction_construction_begin;
        public static double[] transaction_construction_acc;


        public static ConcurrentHashMap<Integer,Double> Avg_WaitTime;
        public static ConcurrentHashMap<Integer,Double> Avg_CommitTime;
        public static void Initialize(int tthread_num) {
            upstream_backup_time = new DescriptiveStatistics[tthread_num + 1];
            upstream_backup_begin = new long[tthread_num + 1];
            upstream_backup_acc = new double[tthread_num + 1];
            for (int i = 0 ; i <= tthread_num; i++){
                upstream_backup_time[i] = new DescriptiveStatistics();
                upstream_backup_begin[i] = 0;
                upstream_backup_acc[i] = 0;
            }
            Help_Log = new DescriptiveStatistics[tthread_num];
            Help_Log_begin = new long[tthread_num];
            Help_Log_backup_acc = new double[tthread_num];
            transaction_construction_time = new DescriptiveStatistics[tthread_num];
            transaction_construction_begin = new long[tthread_num];
            transaction_construction_acc = new double[tthread_num];
            transaction_run_time = new DescriptiveStatistics[tthread_num];
            transaction_post_time = new DescriptiveStatistics[tthread_num];
            transaction_begin_time = new long[tthread_num];
            transaction_post_begin_time = new long[tthread_num];
            for (int i = 0 ; i < tthread_num; i++){
                Help_Log[i] = new DescriptiveStatistics();
                Help_Log_begin[i] = 0;
                Help_Log_backup_acc[i] = 0;
                transaction_construction_time[i] = new DescriptiveStatistics();
                transaction_construction_begin[i] = 0;
                transaction_construction_acc[i] = 0;
                transaction_run_time[i]  =new DescriptiveStatistics();
                transaction_begin_time[i] = 0;
                transaction_post_time[i] = new DescriptiveStatistics();
                transaction_post_begin_time[i] = 0;
            }
            //not used
        }
    }
    //Snapshot and Wal FileSize
    public static DescriptiveStatistics[] snapshot_file_size;
    public static DescriptiveStatistics[] wal_file_size;
    public static long[] previous_wal_file_size;
    //Recovery BreakDown
    static class Recovery_Breakdown {
        //Input-load
        public static DescriptiveStatistics input_load_time;
        public static long input_load_time_begin;
        //State-load
        public static DescriptiveStatistics state_load_time;
        public static long state_load_time_begin;
        //Align-time
        public static DescriptiveStatistics align_time;
        public static long align_time_begin;
        //RedoLog
        public static DescriptiveStatistics RedoLog_time;
        public static long RedoLog_time_begin;
        //Re-execute
        public static DescriptiveStatistics ReExecute_time;
        public static long ReExecute_time_begin;
        public static void Initialize() {
            input_load_time = new DescriptiveStatistics();
            state_load_time = new DescriptiveStatistics();
            align_time = new DescriptiveStatistics();
            RedoLog_time = new DescriptiveStatistics();
            ReExecute_time = new DescriptiveStatistics();
        }
    }
    public static final LocalFileSystem localFileSystem=new LocalFileSystem();
    public static int FT;
    public static void Initialize(int partition_num,int tthread_num,int FT_) {
        snapshot_file_size = new DescriptiveStatistics[partition_num];
        wal_file_size = new DescriptiveStatistics[partition_num];
        previous_wal_file_size = new long[partition_num];

        for(int i=0;i<partition_num;i++){
            previous_wal_file_size[i]=0;
            snapshot_file_size[i] = new DescriptiveStatistics();
            wal_file_size[i] = new DescriptiveStatistics();
        }
        FT = FT_;
    }
}
