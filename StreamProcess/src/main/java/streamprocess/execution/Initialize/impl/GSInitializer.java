package streamprocess.execution.Initialize.impl;

import System.util.Configuration;
import applications.events.InputDataGenerator.ImplDataGenerator.GSDataGenerator;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.table.RecordSchema;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.IntDataBox;
import engine.table.datatype.DataBoxImpl.StringDataBox;
import engine.table.tableRecords.SchemaRecord;
import engine.table.tableRecords.TableRecord;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.Initialize.TableInitilizer;
import utils.TransactionalProcessConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static UserApplications.CONTROL.NUM_ITEMS;
import static UserApplications.CONTROL.enable_states_partition;
import static UserApplications.constants.GrepSumConstants.Constant.VALUE_LEN;
import static utils.PartitionHelper.getPartition_interval;

public class GSInitializer extends TableInitilizer{
    private static final Logger LOG = LoggerFactory.getLogger(GSInitializer.class);
    protected int partition_interval;
    protected int range_interval;

    public GSInitializer(Database db, double scale_factor, double theta, int partition_num, Configuration config) {
        super(db, scale_factor, theta, partition_num, config);
        partition_interval=getPartition_interval();
        range_interval = (int) Math.ceil(NUM_ITEMS / (double) config.getInt("tthread"));//NUM_ITEMS / tthread;
        this.dataGenerator = new GSDataGenerator();
        dataGenerator.initialize(dataRootPath,config);
    }

    @Override
    public void creates_Table(Configuration config) throws IOException {
        if(enable_states_partition){
            for(int i = 0; i< partition_num; i++){
                RecordSchema s = MicroTableSchema();
                db.createTable(s, "MicroTable_"+i, TransactionalProcessConstants.DataBoxTypes.STRING);
            }
        }else{
            RecordSchema s = MicroTableSchema();
            db.createTable(s, "MicroTable", TransactionalProcessConstants.DataBoxTypes.STRING);
        }
        db.createTableRange(1);
        this.Prepare_input_event();
    }
    private RecordSchema MicroTableSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new StringDataBox());
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }
    private void insertMicroRecord(int key, String value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new StringDataBox(value, value.length()));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            if (enable_states_partition){
                db.InsertRecord("MicroTable"+"_"+getPartitionId(key), new TableRecord(schemaRecord));
            }else {
                db.InsertRecord("MicroTable", new TableRecord(schemaRecord));
            }
        } catch (DatabaseException | IOException e) {
            e.printStackTrace();
        }
    }
    private int getPartitionId(int key) {
        return (int) Math.floor(key / (double) partition_interval);//NUM_ITEMS / tthread;
    }
    @Override
    public void loadDB(int thread_id, TopologyContext context) {
        int left_bound=thread_id*range_interval;
        int right_bound;
        if(thread_id==context.getNUMTasks()-1){//last executor need to handle right-over
            right_bound=NUM_ITEMS;
        }else{
            right_bound=(thread_id+1)*range_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value);
        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    @Override
    public void reloadDB(List<Integer> rangeId) {
        for (int key = 0; key < NUM_ITEMS ; key++) {
            if(rangeId.contains(getPartitionId(key))){
                String value = GenerateValue(key);
                assert value.length() == VALUE_LEN;
                insertMicroRecord(key, value);
            }
        }
    }
    public static String GenerateValue(int key) {
        return rightpad(String.valueOf(key), VALUE_LEN);
    }
    private static String rightpad(@NotNull String text, int length) {
        return StringUtils.rightPad(text, length); // Returns "****foobar"
//        return String.format("%-" + length + "." + length + "s", text);
    }
}
