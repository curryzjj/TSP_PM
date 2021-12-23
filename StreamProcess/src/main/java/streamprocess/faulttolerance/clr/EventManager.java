package streamprocess.faulttolerance.clr;


import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import com.ning.compress.lzf.util.LZFFileOutputStream;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import static UserApplications.CONTROL.*;

/**
 * Commit and redo the events
 */
public class EventManager {
    private final Logger LOG= LoggerFactory.getLogger(EventManager.class);
    protected final String split_exp = "marker";
    private final LocalFileSystem localFS=new LocalFileSystem();
    public static ExecutorService clrExecutor;
    public EventManager(){

    }
    public class PersistLogicTask implements Callable<Boolean>{
        private int partition_id;
        private ConcurrentLinkedQueue<ComputationLogic> computationTasks;
        private long eventTaskId;
        private File clrFile;
        public PersistLogicTask(int partition_id, long eventTaskId, ConcurrentLinkedQueue<ComputationLogic> logics, Path parentPath){
            this.partition_id=partition_id;
            this.eventTaskId=eventTaskId;
            this.computationTasks=logics;
            Path path=new Path(parentPath,"CLR"+partition_id);
            clrFile=localFS.pathToFile(path);
        }
        @Override
        public Boolean call() throws Exception {
            persistComputationLogics(this.clrFile,this.computationTasks,this.eventTaskId,this.partition_id);
            return true;
        }
    }
    public void persistComputationLogics(File clrFile,ConcurrentLinkedQueue<ComputationLogic> logics,long eventTaskId,int partitionId) throws IOException {
        FileWriter Fw= null;
        Fw = new FileWriter(clrFile,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for (ComputationLogic logic:logics){
           // bw.write(logic.toString(partitionId));
            bw.write( "\n");
        }
        bw.write(split_exp+" ");
        bw.write(String.valueOf(eventTaskId));
        bw.write( "\n");
        bw.flush();
        bw.close();
        Fw.close();
    }
    private void Init_PersistEventLogic(List<PersistLogicTask> callables,Path current,EventsTask eventsTask){
        for (int i=0;i<partition_num;i++){
            callables.add(new PersistLogicTask(i,eventsTask.getTaskId(), eventsTask.getComputationLogicsQueues(), current));
        }
    }
    public void persistEventsLogic(Path currentPath,EventsTask eventsTask) throws IOException, InterruptedException {
        if(enable_states_partition){
            List<PersistLogicTask> callables=new ArrayList<>();
            Init_PersistEventLogic(callables,currentPath,eventsTask);
            clrExecutor.invokeAll(callables);
        }else{
            Path path=new Path(currentPath,"CLR");
            File clrFile=localFS.pathToFile(path);
            persistComputationLogics(clrFile,eventsTask.getComputationLogicsQueues(), eventsTask.getTaskId(),0);
        }
    }

    private class PersistEventTask implements Callable<Boolean>{
        private int partition_id;
        private ConcurrentLinkedQueue<ComputationTask> computationTasks;
        private long eventTaskId;
        private File clrFile;
        public PersistEventTask(int partition_id, long eventTaskId, ConcurrentLinkedQueue<ComputationTask> tasks, Path parentPath){
            this.partition_id=partition_id;
            this.eventTaskId=eventTaskId;
            this.computationTasks=tasks;
            Path path=new Path(parentPath,"CLR"+partition_id);
            clrFile=localFS.pathToFile(path);
        }
        @Override
        public Boolean call() throws Exception {
            persistComputationTasks(this.clrFile,this.computationTasks,this.eventTaskId);
            return true;
        }
    }
    private void Init_PersistEventTask(List<PersistEventTask> callables,Path current,EventsTask eventsTask){
        for (int i=0;i<partition_num;i++){
            callables.add(new PersistEventTask(i,eventsTask.getTaskId(),eventsTask.getComputationTasksByPartitionId(i),current));
        }
    }
    private void persistComputationTasks(File clrFile,ConcurrentLinkedQueue<ComputationTask> tasks, long eventTaskId ) throws IOException {
        FileWriter Fw= null;
        Fw = new FileWriter(clrFile,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for (ComputationTask task:tasks){
            bw.write(String.valueOf(eventTaskId));
            bw.write(task.toString());
            bw.write( "\n");
        }
        bw.write(split_exp+" ");
        bw.write(String.valueOf(eventTaskId));
        bw.write( "\n");
        bw.flush();
        bw.close();
        Fw.close();
    }
    public void persistEventsTask(Path currentPath,EventsTask eventsTask) throws IOException, InterruptedException {
        if(enable_states_partition){
            List<PersistEventTask> callables=new ArrayList<>();
            Init_PersistEventTask(callables,currentPath,eventsTask);
            clrExecutor.invokeAll(callables);
        }else{
            Path path=new Path(currentPath,"CLR");
            File clrFile=localFS.pathToFile(path);
            persistComputationTasks(clrFile,eventsTask.getComputationTasksByPartitionId(0), eventsTask.getTaskId());
        }
    }
    public void loadComputationTasks(List<Integer> partitionIds, Path path, Map<Integer, Queue> queues) throws FileNotFoundException {
        File file = null;
        if(enable_states_partition){
            file=localFS.pathToFile(new Path(path,"CLR"+partitionIds.get(0)));
        }else{
            file=localFS.pathToFile(new Path(path,"CLR"));
        }
        int i=1;
        Scanner scanner=new Scanner(file,"UTF-8");
        while(scanner.hasNextLine()){
            String computationTask=scanner.nextLine();
            String[] split = computationTask.split(" ");
            if(split[0].equals("marker")){
                ComputationTask task=new ComputationTask(true,false);
                for (int a=1;a<queues.size()+1;a++){
                    queues.get(a).offer(task);
                }
            }else{
                ComputationTask task=new ComputationTask(Long.parseLong(split[0]),split[1],split[2],split[3]);
                queues.get(i).offer(task);
            }
            i++;
            if (i>queues.size()){
                i=1;
            }
        }
        ComputationTask task=new ComputationTask(false,true);
        for (int a=1;a<queues.size()+1;a++){
            queues.get(a).offer(task);
        }

    }

    private void persistBidTask(File clrFile,ConcurrentLinkedQueue<Long> bids,long eventTaskId) throws IOException {
        //OutputStream outputStream=new LZFFileOutputStream(clrFile,true);
        LocalDataOutputStream outputStream=new LocalDataOutputStream(clrFile);
        StringBuilder stringBuilder=new StringBuilder();
        for (Long bid:bids){
            stringBuilder.append(bid.toString());
            stringBuilder.append(" ");
        }
        stringBuilder.append(split_exp+" ");
        stringBuilder.append(eventTaskId);
        outputStream.write(stringBuilder.toString().getBytes(StandardCharsets.UTF_8).length);
        outputStream.write(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
    }
    public void persistBid(Path currentPath, EventsTask eventsTask) throws InterruptedException, IOException {
        if(enable_states_partition){
            List<PersistEventTask> callables=new ArrayList<>();
            Init_PersistEventTask(callables,currentPath,eventsTask);
            clrExecutor.invokeAll(callables);
        }else{
            Path path=new Path(currentPath,"CLR");
            File clrFile=localFS.pathToFile(path);
            persistBidTask(clrFile,eventsTask.getBidQueuesByPartitionId(0), eventsTask.getTaskId());
        }
    }
    public void commitEventId(Path currentPath,EventsTask eventsTask) throws IOException {
        Path path=new Path(currentPath,"CLR");
        File clrFile=localFS.pathToFile(path);
        FileWriter Fw= null;
        Fw = new FileWriter(clrFile,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        bw.write(split_exp+" ");
        bw.write(String.valueOf(eventsTask.getTaskId()));
        bw.write( "\n");
        bw.flush();
        bw.close();
        Fw.close();
    }
}
