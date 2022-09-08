package streamprocess.execution.Initialize.impl;

import System.util.Configuration;
import applications.events.InputDataGenerator.ImplDataGenerator.OBDataGenerator;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.table.RecordSchema;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.IntDataBox;
import engine.table.datatype.DataBoxImpl.LongDataBox;
import engine.table.tableRecords.SchemaRecord;
import engine.table.tableRecords.TableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.Initialize.TableInitilizer;
import utils.TransactionalProcessConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import static UserApplications.CONTROL.NUM_ITEMS;
import static UserApplications.CONTROL.enable_states_partition;
import static UserApplications.constants.OnlineBidingSystemConstants.Constant.MAX_Price;

public class OBInitiallizer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(OBInitiallizer.class);
    protected int partition_interval;
    protected int range_interval;
    SplittableRandom rnd=new SplittableRandom(1234);
    public OBInitiallizer(Database db, double scale_factor, double theta, int partition_num, Configuration config) {
        super(db, scale_factor, theta, partition_num, config);
        range_interval=(int) Math.ceil(NUM_ITEMS / (double) config.getInt("tthread"));//NUM_ITEMS / tthread;
        partition_interval = (int) Math.ceil(NUM_ITEMS / (double) partition_num);//NUM_ITEMS / tthread;
        this.dataGenerator = new OBDataGenerator();
        dataGenerator.initialize(dataRootPath,config);
    }
    private RecordSchema Goods() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new LongDataBox());

        fieldNames.add("ID");//OB
        fieldNames.add("Price");
        fieldNames.add("Qty");

        return new RecordSchema(fieldNames, dataBoxes);
    }
    @Override
    public void creates_Table(Configuration config) throws IOException {
        if(enable_states_partition){
            for (int i = 0; i< partition_num; i++){
                RecordSchema s = Goods();
                db.createTable(s, "goods_"+i, TransactionalProcessConstants.DataBoxTypes.LONG);
            }
        }else{
            RecordSchema s = Goods();
            db.createTable(s, "goods", TransactionalProcessConstants.DataBoxTypes.LONG);
        }
        db.createTableRange(1);
        this.Prepare_input_event();
    }

    @Override
    public void loadDB(int thread_id, TopologyContext context) {
        int left_bound = thread_id * range_interval;
        int right_bound;
        if (thread_id == context.getNUMTasks() - 1) {//last executor need to handle left-over
            right_bound = NUM_ITEMS;
        } else {
            right_bound = (thread_id + 1) * range_interval;
        }

        for (int key = left_bound; key < right_bound; key++) {
            insertItemRecords(key, 100);
        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    @Override
    public void reloadDB(List<Integer> rangeId) {
        for (int key = 0; key < NUM_ITEMS ; key++) {
            if(rangeId.contains(getPartitionId(key))){
                insertItemRecords(key, 100);
            }
        }
    }
    /**
     *
     * 4 + 8 + 8
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertItemRecords(int key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            if(enable_states_partition){
                db.InsertRecord("goods_"+getPartitionId(key), new TableRecord(schemaRecord));
            }else {
                db.InsertRecord("goods", new TableRecord(schemaRecord));

            }
        } catch (DatabaseException | IOException e) {
            e.printStackTrace();
        }
    }
    private int getPartitionId(int key) {
        return (int) Math.floor(key / (double) partition_interval);//NUM_ITEMS / tthread;
    }
}
