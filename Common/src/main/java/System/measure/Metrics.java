package System.measure;

import System.FileSystem.ImplFS.LocalFileSystem;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Metrics {
    static class Performance {
        public static ConcurrentHashMap<Integer,Double> Avg_throughput;
        public static ConcurrentHashMap<Integer,Double> Avg_latency;
        public static ConcurrentHashMap<Integer,Double> Tail_latency;
        public static ConcurrentHashMap<Integer, List<Double>> latency_map;
        public static ConcurrentHashMap<Integer,List<Double>> throughput_map;
        public static void Initialize() {
            Avg_throughput = new ConcurrentHashMap<>();
            Tail_latency = new ConcurrentHashMap<>();
            Avg_latency = new ConcurrentHashMap<>();
            latency_map = new ConcurrentHashMap<>();
            throughput_map = new ConcurrentHashMap<>();
        }
    }
    static class Runtime_Breakdown{
        public static final DescriptiveStatistics FTM_start_ack_time =new DescriptiveStatistics();
        public static final DescriptiveStatistics FTM_finish_ack_time =new DescriptiveStatistics();
        public static DescriptiveStatistics input_store_time=new DescriptiveStatistics();
        public static final DescriptiveStatistics persist_time=new DescriptiveStatistics();
        public static DescriptiveStatistics[] transaction_run_time;
        public static DescriptiveStatistics[] event_post_time;
        public static ConcurrentHashMap<Integer,Double> Avg_WaitTime;
        public static ConcurrentHashMap<Integer,Double> Avg_CommitTime;
        public static long[] bolt_register_ack_time;
        public static long[] bolt_receive_ack_time;
        public static long[] transaction_begin_time;
        public static long[] post_begin_time;
        public static long input_store_begin_time;
        public static long FTM_finish_time;
        public static void Initialize(int tthread_num) {
            transaction_run_time=new DescriptiveStatistics[tthread_num];
            event_post_time=new DescriptiveStatistics[tthread_num];
            Avg_WaitTime=new ConcurrentHashMap<>();
            Avg_CommitTime=new ConcurrentHashMap<>();
            for (int i=0;i<tthread_num;i++){
                transaction_run_time[i]=new DescriptiveStatistics();
                event_post_time[i]=new DescriptiveStatistics();
            }
            bolt_register_ack_time =new long[tthread_num];
            bolt_receive_ack_time =new long[tthread_num];
            transaction_begin_time=new long[tthread_num];
            post_begin_time=new long[tthread_num];
        }
    }

    public static DescriptiveStatistics[] snapshot_file_size;
    public static DescriptiveStatistics[] wal_file_size;

    public static long[] previous_wal_file_size;
    public static final DescriptiveStatistics recovery_time=new DescriptiveStatistics();
    public static final DescriptiveStatistics transaction_abort_time=new DescriptiveStatistics();


    public static long input_reload_begin_time;
    public static double input_reload_time;

    public static long recovery_begin_time;
    public static long persist_begin_time;
    public static long transaction_abort_begin_time;
    public static double reloadDB;
    public static long reloadDB_start_time;
    public static final LocalFileSystem localFileSystem=new LocalFileSystem();
    public static int FT;
    public static int replayData;
    public static void Initialize(int partition_num,int tthread_num,int FT_) {
        snapshot_file_size = new DescriptiveStatistics[partition_num];
        wal_file_size=new DescriptiveStatistics[partition_num];
        previous_wal_file_size=new long[partition_num];

        for(int i=0;i<partition_num;i++){
            previous_wal_file_size[i]=0;
            snapshot_file_size[i] = new DescriptiveStatistics();
            wal_file_size[i]=new DescriptiveStatistics();
        }
        FT=FT_;
    }
}
