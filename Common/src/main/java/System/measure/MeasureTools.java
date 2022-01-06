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
    private static DescriptiveStatistics input_store_time=new DescriptiveStatistics();
    private static DescriptiveStatistics twoPC_commit_time=new DescriptiveStatistics();
    private static long[] previous_wal_file_size;
    private static final DescriptiveStatistics recovery_time=new DescriptiveStatistics();
    private static final DescriptiveStatistics transaction_abort_time=new DescriptiveStatistics();
    private static final DescriptiveStatistics persist_time=new DescriptiveStatistics();
    private static DescriptiveStatistics[] transaction_run_time;
    private static DescriptiveStatistics[] event_post_time;
    private static long[] bolt_register_ack_time;
    private static long[] bolt_receive_ack_time;
    private static long[] transaction_begin_time;
    private static long[] post_begin_time;
    private static long input_store_begin_time;
    private static long input_reload_begin_time;
    private static double input_reload_time;
    private static long commitStartTime;
    private static long FTM_finish_time;
    private static long recovery_begin_time;
    private static long persist_begin_time;
    private static long transaction_abort_begin_time;
    private static double reloadDB;
    private static long reloadDB_start_time;
    private static final LocalFileSystem localFileSystem=new LocalFileSystem();
    private static int FT;
    private static int replayData;
    public MeasureTools(int partition_num,int tthread_num,int FT_) {
        serialization_time = new DescriptiveStatistics[partition_num];
        snapshot_file_size = new DescriptiveStatistics[partition_num];
        wal_file_size=new DescriptiveStatistics[partition_num];
        previous_wal_file_size=new long[partition_num];
        transaction_run_time=new DescriptiveStatistics[tthread_num];
        event_post_time=new DescriptiveStatistics[tthread_num];
         for(int i=0;i<partition_num;i++){
            previous_wal_file_size[i]=0;
            serialization_time[i] = new DescriptiveStatistics();
            snapshot_file_size[i] = new DescriptiveStatistics();
            wal_file_size[i]=new DescriptiveStatistics();
        }
        for (int i=0;i<tthread_num;i++){
            transaction_run_time[i]=new DescriptiveStatistics();
            event_post_time[i]=new DescriptiveStatistics();
        }
        bolt_register_ack_time =new long[tthread_num];
        bolt_receive_ack_time =new long[tthread_num];
        transaction_begin_time=new long[tthread_num];
        post_begin_time=new long[tthread_num];
        FT=FT_;
    }
    public static void setReplayData(int num){
        replayData=num;
    }
    public static void twoPC_commit_begin(long time){
        commitStartTime=time;
    }
    public static void twoPC_commit_finish(long time){
        twoPC_commit_time.addValue((time-commitStartTime)/1E6);
    }
    public static void Input_store_begin(long time){
        input_store_begin_time=time;
    }
    public static void Input_store_finish(){
        input_store_time.addValue((System.nanoTime()-input_store_begin_time)/1E6);
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
    public static void startTransaction(int threadId,long time){
        transaction_begin_time[threadId]=time;
    }
    public static void finishTransaction(int threadId,long time){
        transaction_run_time[threadId].addValue((time-transaction_begin_time[threadId])/1E6);
    }
    public static void startPost(int threadId,long time){
        post_begin_time[threadId]=time;
    }
    public static void finishPost(int threadId,long time){
        event_post_time[threadId].addValue((time-post_begin_time[threadId])/1E6);
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
    public static void setSnapshotFileSize(Set<Path> paths){
        int i=0;
        for (Path path:paths){
            File snapshotFile=localFileSystem.pathToFile(path);
            snapshot_file_size[i].addValue(snapshotFile.length()/(1024*1024));
            i++;
        }
    }
    public static void setWalFileSize(Path path){
        if(wal_file_size.length==1){
            File walFile=localFileSystem.pathToFile(new Path(path,"WAL"));
            wal_file_size[0].addValue((walFile.length()-previous_wal_file_size[0])/(1024*1024));
            previous_wal_file_size[0]=walFile.length();
        }else{
            for (int i=0;i<wal_file_size.length;i++){
                File walFile=localFileSystem.pathToFile(new Path(path,"WAL_"+i));
                wal_file_size[i].addValue((walFile.length()-previous_wal_file_size[i])/(1024*1024));
                previous_wal_file_size[i]=walFile.length();
            }
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
        sb.append("=======Input store Time Details=======");
        sb.append("\n" + input_store_time.toString() + "\n");
        sb.append("=======Persist Time Details=======");
        sb.append("\n" + persist_time.toString() + "\n");
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
            //sb.append("=======Thread"+i+" transaction running time Details=======");
            //sb.append("\n" + transaction_run_time[i].toString() + "\n");
            Total_time=Total_time+transaction_run_time[i].getMean();
        }
        sb.append("Avg transaction_run_time: "+Total_time/transaction_run_time.length+"\n");
        LOG.info(sb.toString());
        for (int i=0;i< event_post_time.length;i++){
            //sb.append("=======Thread"+i+" Event post time Details=======");
            //sb.append("\n" + event_post_time[i].toString() + "\n");
            Total_time=Total_time+event_post_time[i].getMean();
        }
        sb.append("Avg post_run_time: "+Total_time/event_post_time.length+"\n");
        sb.append("=======2PC commit time=======");
        sb.append("\n" + twoPC_commit_time + "\n");
        LOG.info(sb.toString());
    }
}
