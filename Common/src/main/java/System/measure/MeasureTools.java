package System.measure;

import System.FileSystem.Path;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static System.measure.Metrics.*;
import static System.measure.Metrics.Recovery_Breakdown.*;
import static System.measure.Metrics.Runtime_Breakdown.*;
import static UserApplications.CONTROL.PARTITION_NUM;
import static UserApplications.CONTROL.enable_states_lost;
import static java.nio.file.StandardOpenOption.APPEND;

public class MeasureTools {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureTools.class);
    public MeasureTools(int partition_num,int tthread_num , int FT_) {
        Metrics.Initialize(partition_num,tthread_num,FT_);
        Metrics.Performance.Initialize();
        Metrics.Runtime_Breakdown.Initialize(tthread_num);
        Metrics.Recovery_Breakdown.Initialize();
    }
    //Wait time measure
    public static void SetWaitTime(double time) {
        wait_time.addValue(time);
    }
    //Input Store time measure
    public static void Input_store_begin(long time){
        input_store_begin_time = time;
    }
    public static void Input_store_finish(){
        input_store_time.addValue((System.nanoTime() - input_store_begin_time) / 1E6);
    }
    //Upstream backup time measure
    public static void Upstream_backup_begin(int executorId, long time) {
        upstream_backup_begin[executorId] = time;
    }
    public static void Upstream_backup_acc(int executorId, long time) {
        upstream_backup_acc[executorId] = upstream_backup_acc[executorId] + (time - upstream_backup_begin[executorId]) / 1E6;
    }
    public static void Upstream_backup_finish_acc(int executorId) {
        upstream_backup_time[executorId].addValue(upstream_backup_acc[executorId]);
        upstream_backup_acc[executorId] = 0;
    }
    //Snapshot time measure
    public static void startSnapshot(long time){
        Snapshot_begin_time = time;
    }
    public static void finishSnapshot(long time){
        Snapshot_time.addValue((time- Snapshot_begin_time) / 1E6);
    }
    //HelpLog
    // 1.Wal
    public static void startWAL(long time){
        WAL_begin_time = time;
    }
    public static void finishWAL(long time){
        Wal_time.addValue((time - WAL_begin_time) / 1E6);
    }
    // 2.CLR
    public static void HelpLog_backup_begin(int threadId, long time) {
        Help_Log_begin[threadId] = time;
    }
    public static void HelpLog_backup_acc(int threadId, long time) {
        Help_Log_backup_acc[threadId] = Help_Log_backup_acc[threadId] + (time - Help_Log_begin[threadId]) / 1E6;
    }
    public static void HelpLog_finish_acc(int threadId) {
        Help_Log[threadId].addValue(Help_Log_backup_acc[threadId]);
        Help_Log_backup_acc[threadId] = 0;
    }
    //Txn_time
    public static void startTransaction(int threadId,long time){
        transaction_begin_time[threadId]=time;
    }
    public static void finishTransaction(int threadId,long time){
        transaction_run_time[threadId].addValue((time-transaction_begin_time[threadId])/1E6);
    }
    //Txn_Post_Time
    public static void startPostTransaction(int threadId, long time) {
        transaction_post_begin_time[threadId] = time;
    }
    public static void finishPostTransaction(int threadId, long time) {
        transaction_post_time[threadId].addValue((time-transaction_post_begin_time[threadId]) / 1E6);
    }
    //Txn_Construction_Time
    public static void Transaction_construction_begin(int threadId, long time) {
        transaction_construction_begin[threadId] = time;
    }
    public static void Transaction_construction_acc(int threadId, long time) {
        transaction_construction_acc[threadId] = transaction_construction_acc[threadId] + (time - transaction_construction_begin[threadId]) / 1E6;
    }
    public static void Transaction_construction_finish_acc(int threadId) {
        transaction_construction_time[threadId].addValue(transaction_construction_acc[threadId]);
        transaction_construction_acc[threadId] = 0;
    }
    //Txn_Abort_Time
    public static void startTransactionAbort(int threadId, long time) {
        transaction_abort_begin_time[threadId] = time;
    }
    public static void finishTransactionAbort(int threadId, long time) {
        transaction_run_time[threadId].addValue((time-transaction_abort_begin_time[threadId])/1E6);
    }
    //FileSize Measure
    public static void setSnapshotFileSize(List<Path> paths){
        int i = 0;
        for (Path path:paths){
            File snapshotFile = localFileSystem.pathToFile(path);
            snapshot_file_size[i].addValue(snapshotFile.length() / (1024*1024));
            i++;
        }
    }
    public static void setWalFileSize(Path path){
        if(wal_file_size.length == 1){
            File walFile = localFileSystem.pathToFile(new Path(path,"WAL"));
            wal_file_size[0].addValue((walFile.length() - previous_wal_file_size[0]) / 1024);
            previous_wal_file_size[0] = walFile.length();
        }else{
            for (int i = 0; i < wal_file_size.length; i++){
                File walFile = localFileSystem.pathToFile(new Path(path,"WAL-"+i));
                if (walFile.length() > previous_wal_file_size[i]) {
                    wal_file_size[i].addValue((walFile.length() - previous_wal_file_size[i]) / 1024);
                } else {
                    wal_file_size[i].addValue(walFile.length() / 1024);
                }
                previous_wal_file_size[i] = walFile.length();
            }
        }
    }
    //Input-load
    public static void Input_load_begin(long time) {
        input_load_time_begin = time;
    }
    public static void Input_load_finish(long time) {
        input_load_time.addValue((time - input_load_time_begin) / 1E6);
    }
    //State-load
    public static void State_load_begin(long time) {
        state_load_time_begin = time;
    }
    public static void State_load_finish(long time) {
        state_load_time.addValue((time - state_load_time_begin) / 1E6);
    }
    //Align-time
    public static void Align_time_begin(long time) {
        align_time_begin = time;
    }
    public static void Align_time_finish(long time) {
        align_time.addValue((time - align_time_begin) / 1E6);
    }
    //RedoLog_time
    public static void RedoLog_time_begin(long time) {
        RedoLog_time_begin = time;
    }
    public static void RedoLog_time_finish(long time) {
        RedoLog_time.addValue((time - RedoLog_time_begin) / 1E6);
    }
    //ReExecute_time
    public static void ReExecute_time_begin(long time) {
        ReExecute_time_begin = time;
    }
    public static void ReExecute_time_finish(long time) {
        ReExecute_time.addValue((time - ReExecute_time_begin) / 1E6);
    }


    //Sink Measure
    public static void setAvgThroughput(int threadId, double result) {
        Performance.AvgThroughput.put(threadId, result);
    }
    public static void setLatency(int threadId, DescriptiveStatistics result) {
        Performance.Latency.put(threadId, result);
    }
    public static void setThroughputMap(int threadId, List<Double> result) {
        Performance.throughput_map.put(threadId,result);
    }
    public static void setLatencyMap(int threadId, List<Double> result) {
        Performance.latency_map.put(threadId,result);
    }
    private static void PerformanceReport(String baseDirectory, StringBuilder sb) throws IOException {
        sb.append("\n");
        String statsFolderPath = baseDirectory + "_overview.txt";
        File file = new File(statsFolderPath);
        LOG.info("Dumping stats to...");
        LOG.info(String.valueOf(file.getAbsoluteFile()));
        file.mkdirs();
        if (file.exists())
            file.delete();
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
        double totalThroughput = 0;
        double totalAvgLatency = 0;
        for (double rt : Performance.AvgThroughput.values()) {
            totalThroughput = totalThroughput + rt;
        }
        for (DescriptiveStatistics rt : Performance.Latency.values()) {
            totalAvgLatency = totalAvgLatency + rt.getMean();
        }
        sb.append("=======Throughput=======");
        sb.append("\n" + totalThroughput + "\n");
        sb.append("=======Avg_latency=======");
        sb.append("\n" + totalAvgLatency/Performance.Latency.size() + "\n");
        fileWriter.write("Throughput: " + totalThroughput / Performance.AvgThroughput.size() + "\n");
        fileWriter.write("Avg_latency: " + totalAvgLatency / Performance.Latency.size() + "\n");
        fileWriter.write("Percentile\t Latency\n");
        double percentile[] = new double[]{0.5, 20, 40, 60, 80, 99};
        for (int i = 0; i < percentile.length; i ++){
            double totalTailLatency = 0;
            for (DescriptiveStatistics rt : Performance.Latency.values()) {
                totalTailLatency = totalTailLatency + rt.getPercentile(percentile[i]);
            }
            String output = String.format("%f\t" +
                            "%-10.4f\t"
                    , percentile[i], totalTailLatency / Performance.Latency.size());
            fileWriter.write(output + "\n");
            sb.append("\n" + percentile[i]+ " : " + totalTailLatency / Performance.Latency.size() + "\n");
        }
        FileSizeReport(fileWriter, sb);
        RuntimeBreakdownReport(fileWriter, sb);
        if (enable_states_lost) {
            RecoveryBreakdownReport(fileWriter, sb);
        }
        fileWriter.close();
    }
    public static void FileSizeReport(BufferedWriter fileWriter, StringBuilder sb) throws IOException {
        double snapshotFileSize = 0;
        double walFileSize = 0;
        for (DescriptiveStatistics descriptiveStatistics : snapshot_file_size) {
            snapshotFileSize = snapshotFileSize + descriptiveStatistics.getMean();
        }
        for (DescriptiveStatistics descriptiveStatistics : wal_file_size) {
            walFileSize = walFileSize + descriptiveStatistics.getMean();
        }
        sb.append("=======SnapshotSize=======");
        sb.append("\n" + snapshotFileSize + " MB" +  "\n");
        sb.append("=======WALSize=======");
        sb.append("\n" + walFileSize + " KB" +  "\n");
        fileWriter.write("SnapshotSize: " + snapshotFileSize + " MB" + "\n");
        fileWriter.write("WALSize: " + walFileSize + " KB" +  "\n");
    }
    private static void RuntimeLatencyReport(String baseDirectory) throws IOException {
        String statsFolderPath = baseDirectory + "_latency";
        File file = new File(statsFolderPath);
        LOG.info("Dumping stats to...");
        LOG.info(String.valueOf(file.getAbsoluteFile()));
        file.mkdirs();
        if (file.exists())
            file.delete();
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
        fileWriter.write("time_id\t latency\n");
        int size = 0;
        for (List<Double> list : Performance.latency_map.values()){
            if (list.size() > size) {
                size = list.size();
            }
        }
        for (int i = 0; i < size; i++){
            double latency = 0;
            for (List<Double> list : Performance.latency_map.values()){
                latency = latency + list.get(i);
            }
            String output = String.format("%d\t" +
                            "%-10.4f\t"
                    , i,latency
            );
            fileWriter.write(output + "\n");
        }
        fileWriter.close();
    }
    private static void RuntimeThroughputReport(String baseDirectory) throws IOException {
        String statsFolderPath = baseDirectory + "_throughput";
        File file = new File(statsFolderPath);
        LOG.info("Dumping stats to...");
        LOG.info(String.valueOf(file.getAbsoluteFile()));
        file.mkdirs();
        if (file.exists())
            file.delete();
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
        fileWriter.write("time_id\t  throughput\n");
        int size = 0;
        for (List<Double> list : Performance.throughput_map.values()){
            if (list.size() > size) {
                size = list.size();
            }
        }
        for (int i = 0 ; i < size; i++){
            double latency = 0;
            for (List<Double> list : Performance.throughput_map.values()){
                latency = latency + list.get(i);
            }
            String output = String.format("%d\t" +
                            "%-10.4f\t"
                    , i,latency
            );
            fileWriter.write(output + "\n");
        }
        fileWriter.close();
    }
    private static void RuntimeBreakdownReport(BufferedWriter fileWriter, StringBuilder sb) throws IOException {
        sb.append("\n");
        fileWriter.write("thread_id\t WaitTime \t Input-Store\t Snapshot\t HelpLog \t UpStream \t Construction_time \t Txn_time \t Post_time \n");
        sb.append("\n");
        double helpLog = 0;
        double upstreamBackupTime = 0;
        double transactionConstructionTime = 0;
        double transactionRunTime = 0;
        double transactionPostTime = 0;
        double inputStoreTime = 0 ;
        double snapshotTime = 0;
        if (FT == 1) {
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , wait_time.getMean()
                        , input_store_time.getMean()
                        , Snapshot_time.getMean() - transaction_post_time[threadId].getMean()
                        , Wal_time.getMean() - transaction_post_time[threadId].getMean()
                        , 0.0
                        , transaction_construction_time[threadId].getMean()
                        , transaction_run_time[threadId].getMean()
                        , transaction_post_time[threadId].getMean()
                );
                inputStoreTime = input_store_time.getMean() + inputStoreTime;
                snapshotTime = Snapshot_time.getMean() - transaction_post_time[threadId].getMean() + snapshotTime;
                helpLog = Wal_time.getMean() - transaction_post_time[threadId].getMean() + helpLog;
                transactionConstructionTime = transaction_construction_time[threadId].getMean() + transactionConstructionTime;
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                transactionPostTime = transaction_post_time[threadId].getMean() + transactionPostTime;
                fileWriter.write(output + "\n");
            }
        } else if (FT == 5 || FT == 6) {
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , wait_time.getMean()
                        , input_store_time.getMean()
                        , Snapshot_time.getMean() - transaction_post_time[threadId].getMean()
                        , Help_Log[threadId].getMean()
                        , upstream_backup_time[threadId + 1].getMean() + upstream_backup_time[0].getMean()
                        , transaction_construction_time[threadId].getMean()
                        , transaction_run_time[threadId].getMean()
                        , transaction_post_time[threadId].getMean() - upstream_backup_time[threadId].getMean()
                );
                inputStoreTime = input_store_time.getMean() + inputStoreTime;
                snapshotTime = Snapshot_time.getMean() - transaction_post_time[threadId].getMean() + snapshotTime;
                helpLog = Help_Log[threadId].getMean() + helpLog;
                upstreamBackupTime = upstream_backup_time[threadId].getMean() + upstream_backup_time[0].getMean() + upstreamBackupTime;
                transactionConstructionTime = transaction_construction_time[threadId].getMean() + transactionConstructionTime;
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                transactionPostTime = transaction_post_time[threadId].getMean() - upstream_backup_time[threadId + 1].getMean() + transactionPostTime;
                fileWriter.write(output + "\n");
            }
        } else if (FT == 2){
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , wait_time.getMean()
                        , input_store_time.getMean()
                        , Snapshot_time.getMean() - transaction_post_time[threadId].getMean()
                        , 0.0
                        , 0.0
                        , transaction_construction_time[threadId].getMean()
                        , transaction_run_time[threadId].getMean()
                        , transaction_post_time[threadId].getMean()
                );
                inputStoreTime = input_store_time.getMean() + inputStoreTime;
                snapshotTime = Snapshot_time.getMean() - transaction_post_time[threadId].getMean() + snapshotTime;
                transactionConstructionTime = transaction_construction_time[threadId].getMean() + transactionConstructionTime;
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                transactionPostTime = transaction_post_time[threadId].getMean() + transactionPostTime;
                fileWriter.write(output + "\n");
            }
        } else {
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , wait_time.getMean()
                        , 0.0
                        , 0.0
                        , 0.0
                        , 0.0
                        , transaction_construction_time[threadId].getMean()
                        , transaction_run_time[threadId].getMean()
                        , transaction_post_time[threadId].getMean()
                );
                transactionConstructionTime = transaction_construction_time[threadId].getMean() + transactionConstructionTime;
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                transactionPostTime = transaction_post_time[threadId].getMean() + transactionPostTime;
                fileWriter.write(output + "\n");
            }
        }
        String output = String.format(
                "%-10.2f\t" +
                "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t"
                , wait_time.getMean()
                , inputStoreTime / PARTITION_NUM
                , snapshotTime / PARTITION_NUM
                , helpLog / PARTITION_NUM
                , upstreamBackupTime / PARTITION_NUM
                , transactionConstructionTime / PARTITION_NUM
                , transactionRunTime / PARTITION_NUM
                , transactionPostTime / PARTITION_NUM
        );
        sb.append("thread_id\t WaitTime \t Input-Store\t Snapshot\t HelpLog \t UpStream \t Construction_time \t Txn_time \t Post_time \n");
        sb.append(output);
        fileWriter.write(output + "\n");
    }
    private static void RecoveryBreakdownReport(BufferedWriter fileWriter, StringBuilder sb) throws IOException {
        String output;
        if (FT == 1) {
            output = String.format(
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , input_load_time.getMean()
                    , state_load_time.getMean()
                    , 0.0
                    , RedoLog_time.getMean()
                    , ReExecute_time.getMean()
            );
            fileWriter.write(output + "\n");
        } else if (FT == 2){
            output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , input_load_time.getMean()
                    , state_load_time.getMean()
                    , 0.0
                    , 0.0
                    , ReExecute_time.getMean()
            );
        } else {
            output = String.format(
                    "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , input_load_time.getMean()
                    , state_load_time.getMean()
                    , align_time.getMean()
                    , 0.0
                    , ReExecute_time.getMean()
            );
        }
        fileWriter.write("\n Input-load\t State-load\t Align \t RedoLog \t ReExecute \n");
        fileWriter.write(output + "\n");
        sb.append("\n Input-load\t State-load\t Align \t RedoLog \t ReExecute \n");
        sb.append(output).append("\n");
    }
    public static void METRICS_REPORT(String baseDirectory) throws IOException {
        StringBuilder sb = new StringBuilder();
        PerformanceReport(baseDirectory, sb);
        LOG.info(sb.toString());
        if (enable_states_lost) {
            RuntimeThroughputReport(baseDirectory);
            RuntimeLatencyReport(baseDirectory);
        }
    }
}
