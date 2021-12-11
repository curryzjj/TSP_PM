package System.measure;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

public class MeasureTools {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureTools.class);
    private static final DescriptiveStatistics FTM_start_ack_time =new DescriptiveStatistics();
    private static final DescriptiveStatistics FTM_finish_ack_time =new DescriptiveStatistics();
    private static DescriptiveStatistics[] serialization_time;
    private static DescriptiveStatistics[] snapshot_file_size;
    private static DescriptiveStatistics[] wal_file_size;
    private static long[] previous_wal_file_size;
    private static final DescriptiveStatistics recovery_time=new DescriptiveStatistics();
    private static final DescriptiveStatistics transaction_abort_time=new DescriptiveStatistics();
    private static final DescriptiveStatistics persist_time=new DescriptiveStatistics();
    private static long[] bolt_register_ack_time;
    private static long[] bolt_receive_ack_time;
    private static long FTM_finish_time;
    private static long recovery_begin_time;
    private static long persist_begin_time;
    private static long transaction_abort_begin_time;
    private static final LocalFileSystem localFileSystem=new LocalFileSystem();
    private static int FT;
    public MeasureTools(int partition_num,int tthread_num,int FT_) {
        serialization_time = new DescriptiveStatistics[partition_num];
        snapshot_file_size = new DescriptiveStatistics[partition_num];
        wal_file_size=new DescriptiveStatistics[partition_num];
        previous_wal_file_size=new long[partition_num];
        for(int i=0;i<partition_num;i++){
            previous_wal_file_size[i]=0;
            serialization_time[i] = new DescriptiveStatistics();
            snapshot_file_size[i] = new DescriptiveStatistics();
            wal_file_size[i]=new DescriptiveStatistics();
        }
        bolt_register_ack_time =new long[tthread_num];
        bolt_receive_ack_time =new long[tthread_num];
        FT=FT_;
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
    public static void startPersist(long time){
        persist_begin_time=time;
    }
    public static void finishPersist(long time){
        persist_time.addValue((time-persist_begin_time)/1E6);
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
    public static void setSnapshotFileSize(Set<Path> paths){
        int i=0;
        for (Path path:paths){
            File snapshotFile=localFileSystem.pathToFile(path);
            snapshot_file_size[i].addValue(snapshotFile.length());
            i++;
        }
    }
    public static void setWalFileSize(Path path){
        for (int i=0;i<wal_file_size.length;i++){
            File walFile=localFileSystem.pathToFile(new Path(path,"wal_"+i));
            wal_file_size[i].addValue(walFile.length()-previous_wal_file_size[i]);
            previous_wal_file_size[i]=walFile.length();
        }
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
        sb.append("=======Persist Time Details=======");
        sb.append("\n" + persist_time.toString() + "\n");
        sb.append("=======Recovery Time Details=======");
        sb.append("\n" + recovery_time.toString() + "\n");
        sb.append("=======Undo Time Details=======");
        sb.append("\n" + transaction_abort_time.toString() + "\n");
        switch(FT){
            case 1:
                for (int i=0;i<wal_file_size.length;i++){
                    sb.append("=======Wal"+i+" file size Details=======");
                    sb.append("\n" + wal_file_size[i].toString() + "\n");
                    i++;
                }
            break;
            case 2:
                for (int i=0;i<snapshot_file_size.length;i++){
                    sb.append("=======Snapshot"+i+" file size Details=======");
                    sb.append("\n" + snapshot_file_size[i].toString() + "\n");
                    i++;
                }
            break;
        }
        LOG.info(sb.toString());
    }
}
