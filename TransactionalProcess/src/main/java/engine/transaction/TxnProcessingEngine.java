package engine.transaction;

import System.util.OsUtils;
import engine.storage.StorageManager;
import engine.transaction.common.MyList;
import engine.transaction.common.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SOURCE_CONTROL;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static UserApplications.CONTROL.*;

public class TxnProcessingEngine {
    private static final Logger LOG= LoggerFactory.getLogger(TxnProcessingEngine.class);
    private static TxnProcessingEngine instance=new TxnProcessingEngine();
    public static TxnProcessingEngine getInstance() {
        return instance;
    }
    private StorageManager storageManager;
    //Task_process
    private Integer num_op = -1;
    private Integer first_exe;
    private Integer last_exe;
    private CyclicBarrier barrier;
    private int TOTAL_CORES;
    private ExecutorServiceInstance standalone_engine;
    private HashMap<Integer, ExecutorServiceInstance> multi_engine = new HashMap<>();//one island one engine.
    //initialize
    private String app;
    public void initialize(int size,String app){
        num_op=size;
        this.app=app;
        holder_by_stage = new ConcurrentHashMap<>();
        switch(app){
            case "TP_txn":
                holder_by_stage.put("segment_speed", new Holder_in_range(num_op));
                holder_by_stage.put("segment_cnt", new Holder_in_range(num_op));
                break;
            default:
                throw new UnsupportedOperationException("app not recognized");
        }
    }
    //Operation_chain
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//multi table support. <table_name, Holder_in_range>

    public class Holder{
        public ConcurrentHashMap<String, MyList<Operation>> holder_v1=new ConcurrentHashMap<>();//multi operation support. <key, list of operations>
    }
    public class Holder_in_range{
        public ConcurrentHashMap<Integer,Holder> rangeMap=new ConcurrentHashMap<>();//multi range support. <rangeId, holder>
        public Holder_in_range(Integer num_op){
            int i;
            for (i=0;i<num_op;i++){
                rangeMap.put(i,new Holder());
            }
        }
    }
    public Holder_in_range getHolder(String table_name) {
        return holder_by_stage.get(table_name);
    }

    //Task_Process
    public void engine_init(Integer first_exe,Integer last_exe,Integer executorNode_num,int tp){
        //used in the ExecutionManager
        this.first_exe=first_exe;
        this.last_exe=last_exe;
        num_op=executorNode_num;
        barrier=new CyclicBarrier(num_op);
        if(enable_work_partition){
            if(island==-1){//partition as the core
                for(int i=0;i<tp;i++){
                    multi_engine.put(i,new ExecutorServiceInstance(1));
                }
            }else if(island==-2){//partition as the socket
                int actual_island=tp/CORE_PER_SOCKET;
                int i;
                for (i = 0; i < actual_island; i++) {
                    multi_engine.put(i, new ExecutorServiceInstance(CORE_PER_SOCKET));
                }

                if (tp % CORE_PER_SOCKET != 0) {
                    multi_engine.put(i, new ExecutorServiceInstance(tp % CORE_PER_SOCKET));
                }
            }else{
                throw new UnsupportedOperationException("Unsupported partition strategy");
            }
        }else{
            standalone_engine=new ExecutorServiceInstance(tp);
        }
        TOTAL_CORES = tp;
        LOG.info("Engine initialize:" + " Working Threads:" + tp);
    }
    class ExecutorServiceInstance implements Closeable{
        public ExecutorService executor;
        int range_min;
        int range_max;
        public ExecutorServiceInstance(int tpInstance, int range_min, int range_max) {
            this.range_min = range_min;
            this.range_max = range_max;
            if (enable_work_partition) {
                if (island == -1) {//one core one engine. there's no meaning of stealing.
                    executor = Executors.newSingleThreadExecutor();//one core one engine.
                } else if (island == -2) {//one socket one engine.
                    if (enable_work_stealing) {
                        executor = Executors.newWorkStealingPool(tpInstance);//shared, stealing.
                    } else
                        executor = Executors.newFixedThreadPool(tpInstance);//shared, no stealing.
                } else
                    throw new UnsupportedOperationException();//TODO: support more in future.
            } else {
                if (enable_work_stealing) {
                    executor = Executors.newWorkStealingPool(tpInstance);//shared, stealing.
                } else
                    executor = Executors.newFixedThreadPool(tpInstance);//shared, no stealing.
            }
        }
        public ExecutorServiceInstance(int tpInstance) {
            this(tpInstance, 0, 0);
        }
        @Override
        public void close() throws IOException {
            //TODO:implement shutdown
            engine_shutdown();
        }
    }
    private void engine_shutdown() throws IOException {
        LOG.info("Shutdown Engine!");
        if (enable_work_partition) {
            for (ExecutorServiceInstance engine : multi_engine.values()) {
                engine.close();
            }
        } else {
            standalone_engine.close();
        }
    }

    class Task implements Callable{
        private final Set<Operation> operation_chain;
        public Task(Set<Operation> operation_chain){
            this.operation_chain=operation_chain;
        }
        @Override
        public Object call() throws Exception {
            process((MyList<Operation>) operation_chain, -1);
            return null;
        }
    }
    private void process(MyList<Operation> operation_chain, long mark_ID){
        while (true){
            Operation operation=operation_chain.pollFirst();
            if(operation==null) return;
            process(operation,mark_ID,false);
        }
    }
    private void process(Operation operation, long mark_id, boolean clean) {
        //TODO:after implement the TStreamContent
        switch (operation.accessType){
            case READ_WRITE_READ:

        }
    }

    private int submit_task(int thread_Id,Holder holder,Collection<Callable<Object>> callables,long mark_ID) {
        int sum = 0;
        for (MyList<Operation> operation_chain : holder.holder_v1.values()) {
            if (operation_chain.size() > 0) {
                sum += operation_chain.size();
                boolean flag=Thread.currentThread().isInterrupted();
                if (!flag) {
                    if (enable_engine) {
                        Task task = new Task(operation_chain);
                        if (enable_debug)
                            LOG.trace("Submit operation_chain:" + OsUtils.Addresser.addressOf(operation_chain) + " with size:" + operation_chain.size());
                        callables.add(task);
                    }
                }
            }
        }
        return sum;
    }
    private Integer ThreadToEngine(int thread_Id) {//which executor to invoke the callable
        int rt;
        if (island == -1) {
            rt = (thread_Id);
        } else if (island == -2) {
            rt = thread_Id / CORE_PER_SOCKET;
        } else
            throw new UnsupportedOperationException();
        return rt;
    }

    //evaluation
    public void start_evaluation(int thread_id, long mark_ID) throws InterruptedException {//each operation thread called this function
        //implement the SOURCE_CONTROL sync for all threads to come to this line to ensure chains are constructed for the current batch.
        SOURCE_CONTROL.getInstance().Wait_Start(thread_id);
        int size=evaluation(thread_id,mark_ID);
        //implement the SOURCE_CONTROL sync for all threads to come to this line.
        SOURCE_CONTROL.getInstance().Wait_End(thread_id);
    }
    private int evaluation(int thread_Id,long mark_ID) throws InterruptedException{
        Collection<Callable<Object>> callables=new Vector<>();
        int task=0;
        for (Holder_in_range holder_in_range:holder_by_stage.values()){
            Holder holder = holder_in_range.rangeMap.get(thread_Id);
            task += submit_task(thread_Id, holder, callables, mark_ID);
        }
        if (enable_engine) {
            if (enable_work_partition) {
                multi_engine.get(ThreadToEngine(thread_Id)).executor.invokeAll(callables);
            } else
                standalone_engine.executor.invokeAll(callables);
        }
        return task;
    }
}
