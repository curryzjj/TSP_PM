package streamprocess.execution.Initialize.impl;

import System.util.Configuration;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.table.RecordSchema;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.IntDataBox;
import engine.table.datatype.DataBoxImpl.StringDataBox;
import engine.table.tableRecords.SchemaRecord;
import engine.table.tableRecords.TableRecord;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.Initialize.TableInitilizer;
import utils.TransactionalProcessConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static UserApplications.CONTROL.NUM_ITEMS;
import static UserApplications.CONTROL.enable_states_partition;
import static UserApplications.constants.GrepSumConstants.Constant.VALUE_LEN;
import static applications.events.MicroEvent.GenerateValue;

public class GSInitializer extends TableInitilizer{
    protected int partition_interval;
    public GSInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        partition_interval = (int) Math.ceil(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
    }

    @Override
    public void creates_Table(Configuration config) {
        if(enable_states_partition){
            for(int i=0;i<tthread;i++){
                RecordSchema s = MicroTableSchema();
                db.createTable(s, "MicroTable_"+i, TransactionalProcessConstants.DataBoxTypes.STRING);
            }
        }else{
            RecordSchema s = MicroTableSchema();
            db.createTable(s, "MicroTable", TransactionalProcessConstants.DataBoxTypes.STRING);
        }
        db.createTableRange(1);
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
        for (int key = 0; key < NUM_ITEMS; key++) {
            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value);
        }
        System.out.println();
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
}
