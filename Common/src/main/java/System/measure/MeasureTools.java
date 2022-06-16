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
import java.util.Set;

import static System.measure.Metrics.*;
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
    public static void Upstream_backup_finish(int executorId, long time) {
        upstream_backup_time[executorId].addValue((time - upstream_backup_begin[executorId]) / 1E6);
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
            wal_file_size[0].addValue((walFile.length() - previous_wal_file_size[0]) / (1024*1024));
            previous_wal_file_size[0] = walFile.length();
        }else{
            for (int i = 0; i < wal_file_size.length; i++){
                File walFile = localFileSystem.pathToFile(new Path(path,"WAL_"+i));
                wal_file_size[i].addValue((walFile.length() - previous_wal_file_size[i]) / (1024*1024));
                previous_wal_file_size[i] = walFile.length();
            }
        }
    }


    public static void setReplayData(int num){
        replayData = num;
    }
    public static void bolt_register_Ack(int thread_id,long time){
        bolt_register_ack_time[thread_id]=time;
    }
    public static void FTM_receive_all_Ack(long time){
        FTM_start_ack_time.addValue((time-get_begin_time(bolt_register_ack_time))/1E6);
    }
    public static void FTM_finish_Ack(long time){
        FTM_finish_time=time;
    }
    public static void bolt_receive_ack_time(int thread_id,long time){
        bolt_receive_ack_time[thread_id]=time;
        FTM_finish_ack_time.addValue((time-FTM_finish_time)/1E6);
    }

    public static void startPost(int threadId,long time){
        post_begin_time[threadId]=time;
    }
    public static void finishPost(int threadId,long time){
        event_post_time[threadId].addValue((time-post_begin_time[threadId])/1E6);
    }

    public static void startRecovery(long time){
        recovery_begin_time=time;
    }
    public static void finishRecovery(long time){
        recovery_time.addValue((time-recovery_begin_time)/1E6);
    }
    public static void startUndoTransaction(long time){
        transaction_abort_begin_time=time;
    }
    public static void finishUndoTransaction(long time){
        transaction_abort_time.addValue((time-transaction_abort_begin_time)/1E6);
    }
    public static void startReloadDB(long time){
        reloadDB_start_time=time;
    }
    public static void finishReloadDB(long time){
        reloadDB=(time-reloadDB_start_time)/1E6;
    }
    public static void startReloadInput(long time){
        input_reload_begin_time=time;
    }
    public static void finishReloadInput(long time){
        input_reload_time=(time-input_reload_begin_time)/1E6;
    }
    //Sink Measure
    public static void setAvgThroughput(int threadId, double result) {
        Performance.AvgThroughput.put(threadId,result);
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
    public static void setAvgWaitTime(int threadId, double result) {
        Avg_WaitTime.put(threadId,result);
    }
    public static void setAvgCommitTime(int threadId, double result) {
        Avg_CommitTime.put(threadId,result);
    }
    private static long get_begin_time(long[] times){
        Arrays.sort(times,0,times.length-1);
        return times[0];
    }
    public static void showMeasureResult(){
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("=======FTM Begin Ack Time Details=======");
        sb.append("\n" + FTM_start_ack_time.toString() + "\n");
        sb.append("=======FTM Finish Ack Time Details=======");
        sb.append("\n" + FTM_finish_ack_time.toString() + "\n");
        sb.append("=======Input store Time Details=======");
        sb.append("\n" + input_store_time.toString() + "\n");
        sb.append("=======Persist Time Details=======");
        sb.append("\n" + Snapshot_time.toString() + "\n");
        sb.append("=======ReloadDB Time Details=======");
        sb.append("\n" + reloadDB + "\n");
        sb.append("=======Reload Input Time Details=======");
        sb.append("\n" + input_reload_time + "\n");
        sb.append("=======Recovery Time Details=======");
        sb.append("\n" + recovery_time.toString() + "\n");
        sb.append("=======Lost Data=======");
        sb.append("\n" + replayData + "\n");
        sb.append("=======Undo Time Details=======");
        sb.append("\n" + transaction_abort_time.toString() + "\n");
        switch(FT){
            case 1:
                for (int i=0;i<wal_file_size.length;i++){
                    sb.append("=======Wal"+i+" file size(MB) Details=======");
                    sb.append("\n" + wal_file_size[i].toString() + "\n");
                }
            break;
            case 2:
                for (int i=0;i<snapshot_file_size.length;i++){
                    sb.append("=======Snapshot"+i+" file size(MB) Details=======");
                    sb.append("\n" + snapshot_file_size[i].toString() + "\n");
                }
            break;
        }
        double Total_time=0;
        for (int i=0;i<transaction_run_time.length;i++){
            Total_time=Total_time+transaction_run_time[i].getMean();
        }
        sb.append("Avg transaction_run_time: "+Total_time/transaction_run_time.length+"\n");
        LOG.info(sb.toString());
        for (int i=0;i< event_post_time.length;i++){
            Total_time=Total_time+event_post_time[i].getMean();
        }
        sb.append("Avg post_run_time: "+Total_time/event_post_time.length+"\n");
        sb.append("=======2PC commit time=======");
        //sb.append("\n" + twoPC_commit_time + "\n");
        LOG.info(sb.toString());
    }
    private static void PerformanceReport(String baseDirectory, StringBuilder sb) throws IOException {
        sb.append("\n");
        String statsFolderPath = baseDirectory + "_overview";
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
        double totalTailLatency = 0;
        for (double rt : Performance.AvgThroughput.values()) {
            totalThroughput = totalThroughput+rt;
        }
        for (DescriptiveStatistics rt : Performance.Latency.values()) {
            totalAvgLatency = totalAvgLatency + rt.getMean();
        }
        for (DescriptiveStatistics rt : Performance.Latency.values()) {
            totalTailLatency = totalTailLatency + rt.getPercentile(0.5);
        }
        sb.append("=======Throughput=======");
        sb.append("\n" + totalThroughput + "\n");
        sb.append("=======Avg_latency=======");
        sb.append("\n" + totalAvgLatency/Performance.Latency.size() + "\n");
        fileWriter.write("Throughput: " + totalThroughput + "\n");
        fileWriter.write("Avg_latency: " + totalAvgLatency / Performance.Latency.size() + "\n");
        fileWriter.write("Percentile\t Latency\n");
        double percentile[] = new double[]{0.5, 20, 40, 60, 80, 99};
        for (int i = 0; i < percentile.length; i ++){
            totalTailLatency = 0;
            for (DescriptiveStatistics rt : Performance.Latency.values()) {
                totalTailLatency = totalTailLatency + rt.getPercentile(percentile[i]);
            }
            String output = String.format("%f\t" +
                            "%-10.4f\t"
                    , percentile[i], totalTailLatency / Performance.Latency.size());
            fileWriter.write(output + "\n");
            sb.append("\n" + percentile[i]+ " : " + totalTailLatency / Performance.Latency.size() + "\n");
        }
        if (FT != 0) {
            FileSizeReport(fileWriter, sb);
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
        sb.append("\n" + walFileSize + " MB" +  "\n");
        fileWriter.write("SnapshotSize: " + snapshotFileSize + " MB" + "\n");
        fileWriter.write("WALSize: " + walFileSize + " MB" +  "\n");
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
    private static void RuntimeBreakdownReport(String baseDirectory, StringBuilder sb) throws IOException {
        //TODO: implement later
        sb.append("\n");
        String statsFolderPath = baseDirectory + "_overview";
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
        fileWriter.write("thread_id\t Input-Store\t Snapshot\t HelpLog \t UpStream \t Txn_time");
        double helpLog = 0;
        double upstreamBackupTime = 0;
        double transactionRunTime = 0;
        double inputStoreTime = 0 ;
        double snapshotTime = 0;
        if (FT == 1) {
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , input_store_time.getMean()
                        , Snapshot_time.getMean()
                        , Wal_time.getMean()
                        , 0.0
                        , transaction_run_time[threadId].getMean()
                );
                inputStoreTime = input_store_time.getMean() + inputStoreTime;
                snapshotTime = Snapshot_time.getMean() + snapshotTime;
                helpLog = Wal_time.getMean() + helpLog;
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                fileWriter.write(output + "\n");
            }
        } else if (FT == 3 || FT == 4) {
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , input_store_time.getMean()
                        , Snapshot_time.getMean()
                        , Help_Log[threadId].getMean()
                        , upstream_backup_time[threadId].getMean() + upstream_backup_time[0].getMean()
                        , transaction_run_time[threadId].getMean()
                );
                inputStoreTime = input_store_time.getMean() + inputStoreTime;
                snapshotTime = Snapshot_time.getMean() + snapshotTime;
                helpLog = Help_Log[threadId].getMean() + helpLog;
                upstreamBackupTime = upstream_backup_time[threadId].getMean() + upstream_backup_time[0].getMean() + upstreamBackupTime;
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                fileWriter.write(output + "\n");
            }
        } else if (FT == 2){
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , input_store_time.getMean()
                        , Snapshot_time.getMean()
                        , 0.0
                        , 0.0
                        , transaction_run_time[threadId].getMean()
                );
                inputStoreTime = input_store_time.getMean() + inputStoreTime;
                snapshotTime = Snapshot_time.getMean() + snapshotTime;
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                fileWriter.write(output + "\n");
            }
        } else {
            for (int threadId = 0; threadId < PARTITION_NUM; threadId ++){
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , 0.0
                        , 0.0
                        , 0.0
                        , 0.0
                        , transaction_run_time[threadId].getMean()
                );
                transactionRunTime = transaction_run_time[threadId].getMean() + transactionRunTime;
                fileWriter.write(output + "\n");
            }
        }
        String output = String.format(
                "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t" +
                        "%-10.2f\t"
                , inputStoreTime / PARTITION_NUM
                , snapshotTime / PARTITION_NUM
                , helpLog / PARTITION_NUM
                , upstreamBackupTime / PARTITION_NUM
                , transactionRunTime / PARTITION_NUM
        );
        sb.append("Input-Store\t Snapshot\t HelpLog \t UpStream \t Txn_time \n");
        sb.append(output);
        fileWriter.write(output + "\n");
    }
    public static void METRICS_REPORT(String baseDirectory) throws IOException {
        StringBuilder sb = new StringBuilder();
        PerformanceReport(baseDirectory, sb);
        RuntimeBreakdownReport(baseDirectory, sb);
        LOG.info(sb.toString());
        if (enable_states_lost) {
            RuntimeThroughputReport(baseDirectory);
            RuntimeLatencyReport(baseDirectory);
        }
    }
}
