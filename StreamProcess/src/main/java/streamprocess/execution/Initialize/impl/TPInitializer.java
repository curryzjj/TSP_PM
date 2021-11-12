package streamprocess.execution.Initialize.impl;

import System.util.Configuration;
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

import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;
import static utils.PartitionHelper.getPartition_interval;

public class TPInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TPInitializer.class);

    public TPInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
    }

    @Override
    public void creates_Table(Configuration config) {
        RecordSchema s = SpeedScheme();
        db.createTable(s, "segment_speed", DataBoxTypes.STRING);

        RecordSchema b = CntScheme();
        db.createTable(b, "segment_cnt",DataBoxTypes.OTHERS);
        db.createKeyGroupRange();
    }

    @Override
    public void loadDB(int thread_id, TopologyContext context) {
        //partition on the operator
        int partition_interval=getPartition_interval();
        int left_bound=thread_id*partition_interval;
        int right_bound;
        if(thread_id==context.getNUMTasks()-1){//last executor need to handle right-over
            right_bound=NUM_SEGMENTS;
        }else{
            right_bound=(thread_id+1)*partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {

            String _key = String.valueOf(key);
            insertSpeedRecord(_key, 0);
            insertCntRecord(_key);
        }

        LOG.info("Executor("+ thread_id +") of "+context.getExecutor(context.getThisTaskId()).getOP_full() +" finished loading data from: " + left_bound + " to: " + right_bound);
    }

    private void insertCntRecord(String key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_cnt", new TableRecord(schemaRecord));
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
            db.InsertRecord("segment_speed", new TableRecord(schemaRecord));
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
}
