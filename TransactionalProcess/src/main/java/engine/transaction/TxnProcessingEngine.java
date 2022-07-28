package engine.transaction;

import UserApplications.CONTROL;
import engine.Meta.MetaTypes;
import engine.log.WALManager;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.DoubleDataBox;
import engine.table.datatype.DataBoxImpl.IntDataBox;
import engine.table.tableRecords.SchemaRecord;
import engine.transaction.common.OperationChain;
import engine.transaction.common.Operation;
import engine.transaction.function.AVG;
import engine.log.LogRecord;
import engine.transaction.function.DEC;
import engine.transaction.function.INC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import UserApplications.SOURCE_CONTROL;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static UserApplications.CONTROL.*;

public class TxnProcessingEngine {
    private static final Logger LOG= LoggerFactory.getLogger(TxnProcessingEngine.class);
    private static TxnProcessingEngine instance = new TxnProcessingEngine();
    public static TxnProcessingEngine getInstance() {
        return instance;
    }
    //Task_process
    private Integer num_op = -1;
    private Integer first_exe;
    private Integer last_exe;
    private CyclicBarrier barrier;
    private int TOTAL_CORES;
    private WALManager walManager;
    private long markerId;
    private ExecutorServiceInstance standalone_engine;
    public AtomicBoolean isTransactionAbort = new AtomicBoolean(false);
    private List<Integer> dropTable;
    private HashMap<Integer, ExecutorServiceInstance> multi_engine = new HashMap<>();//one island one engine.
    //initialize
    private String app;
    public void initialize(int size,String app){
        num_op = size;
        this.app = app;
        holder_by_stage = new ConcurrentHashMap<>();
        if (enable_undo_log || enable_wal) {
            this.walManager = new WALManager(PARTITION_NUM);
        }
        this.dropTable = new ArrayList<>();
        switch(app){
            case "TP_txn":
                holder_by_stage.put("segment_speed", new Holder_in_range(num_op));
                holder_by_stage.put("segment_cnt", new Holder_in_range(num_op));
                if (enable_undo_log || enable_wal) {
                    this.walManager.setHolder_by_tableName("segment_speed", PARTITION_NUM);
                    this.walManager.setHolder_by_tableName("segment_cnt", PARTITION_NUM);
                }
                break;
            case "GS_txn":
                holder_by_stage.put("MicroTable", new Holder_in_range(num_op));
                if (enable_undo_log || enable_wal) {
                    this.walManager.setHolder_by_tableName("MicroTable", PARTITION_NUM);
                }
                break;
            case "OB_txn":
                holder_by_stage.put("goods", new Holder_in_range(num_op));
                if (enable_undo_log || enable_wal) {
                    this.walManager.setHolder_by_tableName("goods", PARTITION_NUM);
                }
                break;
            case "SL_txn":
                holder_by_stage.put("T_accounts", new Holder_in_range(num_op));
                holder_by_stage.put("T_assets", new Holder_in_range(num_op));
                if (enable_undo_log || enable_wal) {
                    this.walManager.setHolder_by_tableName("T_accounts", PARTITION_NUM);
                    this.walManager.setHolder_by_tableName("T_assets", PARTITION_NUM);
                }
                break;
            default:
                throw new UnsupportedOperationException("app not recognized");
        }
    }
    //Operation_chain
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//multi table support. <table_name, Holder_in_range>

    public class Holder {
        public ConcurrentHashMap<String, OperationChain<Operation>> holder_v1 = new ConcurrentHashMap<>();//multi operation support. <key, list of operations>
    }
    public class Holder_in_range{
        public ConcurrentHashMap<Integer,Holder> rangeMap = new ConcurrentHashMap<>();//multi range support. <rangeId, holder>
        public Holder_in_range(Integer num_op){
            int i;
            for (i = 0; i < num_op; i++){
                rangeMap.put(i,new Holder());
            }
        }
    }
    public Holder_in_range getHolder(String table_name) {
        return holder_by_stage.get(table_name);
    }
    public void cleanAllOperations(){
        for (Holder_in_range holder_in_range:holder_by_stage.values()){
            for (int thread_Id = 0; thread_Id < num_op; thread_Id ++) {
                Holder holder = holder_in_range.rangeMap.get(thread_Id);
                for (OperationChain operationChain :holder.holder_v1.values()){
                    operationChain.isExecuted = false;
                    operationChain.needAbortHandling.compareAndSet(true, false);
                    operationChain.clear();
                }
            }
        }
    }
    public void cleanOperations(int threadId) {
        for (Holder_in_range holder_in_range:holder_by_stage.values()){
            Holder holder = holder_in_range.rangeMap.get(threadId);
            for (OperationChain operationChain :holder.holder_v1.values()){
                operationChain.isExecuted = false;
                operationChain.needAbortHandling.compareAndSet(true, false);
                operationChain.clear();
            }
        }
    }
    //Task_Process
    public void engine_init(Integer first_exe,Integer last_exe,Integer executorNode_num,int tp){
        //used in the ExecutionManager
        this.first_exe = first_exe;
        this.last_exe = last_exe;
        num_op = executorNode_num;
        barrier = new CyclicBarrier(num_op);
        if(enable_work_partition){
            if(island==-1){//partition as the core
                for(int i = 0; i < tp; i++){
                    multi_engine.put(i, new ExecutorServiceInstance(1));
                }
            }else if(island == -2){//partition as the socket
                int actual_island = tp/CORE_PER_SOCKET;
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
            standalone_engine = new ExecutorServiceInstance(tp);
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
            executor.shutdown();
        }
    }
    public void engine_shutdown() throws IOException {
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
        private final long markId;
        public Task(Set<Operation> operation_chain, long markId){
            this.operation_chain = operation_chain;
            this.markId = markId;
        }
        @Override
        public Object call() throws Exception {
            process((OperationChain<Operation>) operation_chain, markId);
            return null;
        }
    }
    private void process(OperationChain<Operation> operation_chain, long mark_ID){
        if (operation_chain.size() > 0){
            operation_chain.logRecord.setCopyTableRecord(operation_chain.first().s_record);
            operation_chain.isExecuted = true;
            if (enable_undo_log) {
                this.walManager.addLogRecord(operation_chain);
            }
        }
        boolean cleanVersion = true;
        for (Operation operation : operation_chain) {
            if (cleanVersion) {
                operation.s_record.clean_map();
                cleanVersion = false;
            }
            if (operation.operationStateType.equals(MetaTypes.OperationStateType.EXECUTED)
                    || operation.operationStateType.equals(MetaTypes.OperationStateType.ABORTED)
                    || operation.isFailed)
                continue;
            process(operation, mark_ID , operation_chain.getLogRecord(), cleanVersion);
            if (operation.isFailed) {
                operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
                operation.txn_context.checkTransactionAbort(operation, operation_chain);
            } else {
                operation.stateTransition(MetaTypes.OperationStateType.EXECUTED);
            }
        }
    }
    private void process(Operation operation, long markerId, LogRecord logRecord, boolean cleanVersion) {
        switch (operation.accessType){
            case READ_WRITE_READ:
                if (app.equals("TP_txn")){
                    this.TP_TollProcess_Fun(operation);
                }
                if(enable_wal){
                    logRecord.setUpdateTableRecord(operation.s_record);
                }
            break;
            case READ_ONLY:
                if (app.equals("TP_txn")) {
                    assert operation.record_ref != null;
                    //read source_data
                    List<DataBox> srcRecord = operation.s_record.record_.getValues();
                    if (srcRecord.get(1) instanceof DoubleDataBox) {
                        operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(srcRecord.get(1).getDouble())));
                    } else {
                        operation.record_ref.setRecord(new SchemaRecord(new IntDataBox(srcRecord.get(1).getHashSet().size())));
                    }
                } else {
                    SchemaRecord schemaRecord = operation.d_record.record_;
                    operation.record_ref.setRecord(new SchemaRecord(schemaRecord.getValues()));
                }
               //Note that, locking scheme allows directly modifying on original table d_record.
            break;
            case WRITE_ONLY:
                if (app.equals("OB_txn")) {
                    this.OB_Alert_Fun(operation);
                } else if(app.equals("GS_txn")) {
                    this.GS_Write_Fun(operation);
                }
                if(enable_wal){
                    logRecord.setUpdateTableRecord(operation.d_record);
                }
            break;
            case READ_WRITE://read, modify, write
                if(app.equals("SL_txn")){
                    this.SL_Depo_Fun(operation);
                }else if (app.equals("OB_txn")){
                    this.OB_Topping_Fun(operation);
                }
                if(enable_wal){
                    logRecord.setUpdateTableRecord(operation.d_record);
                }
            break;
            case READ_WRITE_COND://read, modify(depends on the condition), write(depend on the condition)
                if(app.equals("SL_txn")){
                    this.SL_Transfer_Fun(operation,false);
                }else if (app.equals("OB_txn")){
                    this.OB_Buying_Fun(operation);
                }
                if(enable_wal){
                    logRecord.setUpdateTableRecord(operation.d_record);
                }
            break;
            case READ_WRITE_COND_READ:
                assert operation.record_ref != null;
                if (app.equals("SL_txn")) {//used in SL
                    SL_Transfer_Fun(operation,true);
                    operation.record_ref.setRecord(operation.condition_records[0].readPreValues(operation.bid));
                }
                if(enable_wal){
                    logRecord.setUpdateTableRecord(operation.d_record);
                }
            break;
            default:throw new UnsupportedOperationException();
        }
    }

    private int submit_task(int thread_Id,Holder holder,Collection<Callable<Object>> callables, long mark_ID) {
        int sum = 0;
        for (OperationChain<Operation> operation_chain : holder.holder_v1.values()) {
            if (operation_chain.isExecuted) {
                boolean needReExecuted = false;
                if (operation_chain.needAbortHandling.get()) {
                    for (Operation operation : operation_chain) {
                        if (operation_chain.failedOperations.contains(operation) && operation.operationStateType.equals(MetaTypes.OperationStateType.EXECUTED)) {
                            needReExecuted = true;
                            operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
                        } else {
                            operation.stateTransition(MetaTypes.OperationStateType.READY);
                        }
                    }
                    if (!needReExecuted) {
                        operation_chain.needAbortHandling.compareAndSet(true, false);
                        continue;
                    }
                } else {
                    continue;
                }
            }
            if (operation_chain.size() > 0) {
                sum = sum + operation_chain.size();
                boolean flag = Thread.currentThread().isInterrupted();
                if (!flag) {
                    if (enable_engine) {
                        Task task = new Task(operation_chain, mark_ID);
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
    public boolean start_evaluation(int thread_id, long mark_ID) throws InterruptedException {//each operation thread called this function
        //implement the SOURCE_CONTROL sync for all threads to come to this line to ensure chains are constructed for the current batch.
        if (enable_wal || enable_undo_log) {
            this.walManager.addLogForBatch(mark_ID);
        }
        this.markerId = mark_ID;
        if (SOURCE_CONTROL.getInstance().Wait_Start(thread_id)) {
            int size = evaluation(thread_id, mark_ID);
        } else {
            return false;
        }
        //implement the SOURCE_CONTROL sync for all threads to come to this line.
        SOURCE_CONTROL.getInstance().Wait_End(thread_id);
        return true;
    }
    public int evaluation(int thread_Id, long mark_ID) throws InterruptedException{
        Collection<Callable<Object>> callables = new Vector<>();
        int task = 0;
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
    public WALManager getWalManager() {
        return walManager;
    }

    public Integer getNum_op() {
        return num_op;
    }
    public List<Integer> getRecoveryRangeId(){
        return dropTable;
    }
    public void mimicFailure(int partitionId) {
        if (enable_wal || enable_checkpoint) {
            for(int i = 0; i < num_op; i++){
                this.dropTable.add(i);
            }
        } else {
            if (!dropTable.contains(partitionId)) {
                dropTable.add(partitionId);
            }
        }
    }
    //Functions to process the operation
    private boolean SL_Depo_Fun(Operation operation){
        if (enable_transaction_abort) {
            if (operation.function.delta_long < 0) {
                this.isTransactionAbort.compareAndSet(false, true);
                operation.isFailed = true;
                return false;
            }
        }
        SchemaRecord srcRecord = operation.s_record.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        SchemaRecord tempo_record = new SchemaRecord(values);
        tempo_record.getValues().get(operation.column_id).incLong(operation.function.delta_long);
        operation.s_record.updateMultiValues(operation.bid, tempo_record);
        CONTROL.randomDelay();
        return true;
    }
    private boolean SL_Transfer_Fun(Operation operation, boolean isRead){
        SchemaRecord preValues;
        if (isRead && operation.record_ref.cnt != 0) {
            preValues = operation.record_ref.getRecord();
        } else {
            preValues = operation.condition_records[0].readPreValues(operation.bid);
        }
        final long sourceBalance = preValues.getValues().get(1).getLong();
        //To contron the abort ratio, wo modify the violation of consistency property
        if (operation.condition.arg1 > 0) {
        //if (operation.condition.arg1 > 0 && sourceBalance > operation.condition.arg1 && sourceBalance > operation.condition.arg2){
            SchemaRecord srcRecord = operation.s_record.readPreValues(operation.bid);
            List<DataBox> values = srcRecord.getValues();
            SchemaRecord tempo_record;
            tempo_record = new SchemaRecord(values);//tempo record
            //apply function.
            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceBalance, operation.function.delta_long);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceBalance, operation.function.delta_long);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.updateMultiValues(operation.bid, tempo_record);
            operation.success[0]=true;
            CONTROL.randomDelay();
            return true;
        }else {
            if (enable_transaction_abort) {
                this.isTransactionAbort.compareAndSet(false, true);
                operation.isFailed = true;
            }
            operation.success[0] = false;
            return false;
        }
    }
    private boolean OB_Buying_Fun(Operation operation) {
        List<DataBox> d_record = operation.condition_records[0].record_.getValues();
        long askPrice = d_record.get(1).getLong();//price
        long left_qty = d_record.get(2).getLong();//available qty;
        long bidPrice = operation.condition.arg1;
        long bid_qty = operation.condition.arg2;
        if (enable_transaction_abort) {
            if (bid_qty < 0) {
                this.isTransactionAbort.compareAndSet(false, true);
                operation.isFailed = true;
                return false;
            }
        }
        // check the preconditions
        if (bidPrice < askPrice || bid_qty > left_qty ) {
            operation.success[0] = false;
        } else {
            d_record.get(2).setLong(left_qty - operation.function.delta_long);//new quantity.
            operation.success[0] = true;
        }
        CONTROL.randomDelay();
        return true;
    }
    private boolean OB_Topping_Fun(Operation operation) {
        SchemaRecord src_record=operation.s_record.record_;
        List<DataBox> values = src_record.getValues();
        if (enable_transaction_abort) {
            if (operation.function.delta_long < 0) {
                this.isTransactionAbort.compareAndSet(false, true);
                operation.isFailed = true;
                return false;
            }
        }
        if(operation.function instanceof INC){
            values.get(operation.column_id).setLong(values.get(operation.column_id).getLong()+operation.function.delta_long);
        }
        CONTROL.randomDelay();
        return true;
    }
    private boolean OB_Alert_Fun(Operation operation) {
        if (enable_transaction_abort) {
            if (operation.value == -1) {
                this.isTransactionAbort.compareAndSet(false, true);
                operation.isFailed = true;
                return false;
            }
        }
        operation.d_record.record_.getValues().get(operation.column_id).setLong(operation.value);
        CONTROL.randomDelay();
        return true;
    }
    private boolean GS_Write_Fun(Operation operation) {
        if (enable_transaction_abort) {
            if (operation.value_list.size() == 0) {
                this.isTransactionAbort.compareAndSet(false, true);
                operation.isFailed = true;
                return false;
            }
        }
        operation.d_record.record_.updateValues(operation.value_list);
        CONTROL.randomDelay();
        return true;
    }
    private boolean TP_TollProcess_Fun(Operation operation) {
        assert operation.record_ref != null;
        //read source_data
        List<DataBox> srcRecord = operation.s_record.record_.getValues();
        if(operation.function instanceof AVG){
            if (enable_transaction_abort) {
                if (operation.function.delta_double >= 180) {
                    this.isTransactionAbort.compareAndSet(false, true);
                    operation.isFailed = true;
                    return false;
                }
            }
            double latestAvgSpeeds = srcRecord.get(1).getDouble();
            double lav;
            if (latestAvgSpeeds == 0) {//not initialized
                lav = operation.function.delta_double;
            } else{
                lav = (latestAvgSpeeds + operation.function.delta_double) / 2;
            }
            srcRecord.get(1).setDouble(lav);//write to state.
            operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
        }else{
            HashSet cnt_segment = srcRecord.get(1).getHashSet();
            cnt_segment.add(operation.function.delta_int);//update hashset; updated state also. TODO: be careful of this.
            operation.record_ref.setRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
        }
        CONTROL.randomDelay();
        return true;
    }
}
