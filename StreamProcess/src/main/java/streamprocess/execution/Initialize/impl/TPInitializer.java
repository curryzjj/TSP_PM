package streamprocess.execution.Initialize.impl;

import System.util.Configuration;
import applications.events.InputDataGenerator.ImplDataGenerator.TPDataGenerator;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.table.RecordSchema;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.DoubleDataBox;
import engine.table.datatype.DataBoxImpl.HashSetDataBox;
import engine.table.datatype.DataBoxImpl.StringDataBox;
import engine.table.tableRecords.SchemaRecord;
import engine.table.tableRecords.TableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.Initialize.TableInitilizer;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static UserApplications.CONTROL.enable_states_partition;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;
import static utils.PartitionHelper.getPartition_interval;

public class TPInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TPInitializer.class);
    protected int partition_interval;
    protected int range_interval;
    public TPInitializer(Database db, double scale_factor, double theta, int partition_num, Configuration config) {
        super(db, scale_factor, theta, partition_num, config);
        partition_interval=getPartition_interval();
        range_interval = (int) Math.ceil(NUM_SEGMENTS / (double) config.getInt("tthread"));//NUM_ITEMS / tthread;
        this.dataGenerator = new TPDataGenerator();
        dataGenerator.initialize(dataRootPath,config);
    }

    @Override
    public void creates_Table(Configuration config) throws IOException {
        if(enable_states_partition){
            for(int i = 0; i < partition_num; i++){
                RecordSchema s = SpeedScheme();
                db.createTable(s, "segment_speed_" + i, DataBoxTypes.DOUBLE);
                RecordSchema b = CntScheme();
                db.createTable(b, "segment_cnt_" + i,DataBoxTypes.OTHERS);
            }
        }else{
            RecordSchema s = SpeedScheme();
            db.createTable(s, "segment_speed", DataBoxTypes.DOUBLE);
            RecordSchema b = CntScheme();
            db.createTable(b, "segment_cnt",DataBoxTypes.OTHERS);
        }
        db.createTableRange(2);
        this.Prepare_input_event();
    }

    @Override
    public void loadDB(int thread_id, TopologyContext context) {
        int left_bound = thread_id * range_interval;
        int right_bound;
        if(thread_id == context.getNUMTasks()-1){//last executor need to handle right-over
            right_bound = NUM_SEGMENTS;
        }else{
            right_bound = (thread_id+1) * range_interval;
        }
        for (int key = left_bound; key < right_bound ; key++) {
            String _key = String.valueOf(key);
            insertSpeedRecord(_key, 0);
            insertCntRecord(_key);
        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    @Override
    public void reloadDB(List<Integer> rangeId) {
        for (int key = 0; key < NUM_SEGMENTS ; key++) {
            String _key = String.valueOf(key);
            if(rangeId.contains(getPartitionId(_key))){
                insertSpeedRecord(_key, 0);
                insertCntRecord(_key);
            }
        }
    }

    private void insertCntRecord(String key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            if(enable_states_partition){
                db.InsertRecord("segment_cnt_" + getPartitionId(key), new TableRecord(schemaRecord));
            }else{
                db.InsertRecord("segment_cnt", new TableRecord(schemaRecord));
            }
        } catch (DatabaseException | IOException e) {
            e.printStackTrace();
        }
    }

    private void insertSpeedRecord(String key, int value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new DoubleDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            if(enable_states_partition){
                db.InsertRecord("segment_speed_" + getPartitionId(key), new TableRecord(schemaRecord));
            }else {
                db.InsertRecord("segment_speed", new TableRecord(schemaRecord));
            }
        } catch (DatabaseException | IOException e) {
            e.printStackTrace();
        }
    }

    private RecordSchema SpeedScheme() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new DoubleDataBox());
        fieldNames.add("Key");
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema CntScheme() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new HashSetDataBox());
        fieldNames.add("Key");
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }
    private int getPartitionId(String key) {
        Integer _key = Integer.valueOf(key);
        return _key / partition_interval;
    }
}
