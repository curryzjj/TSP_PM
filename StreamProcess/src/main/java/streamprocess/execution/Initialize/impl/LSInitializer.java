package streamprocess.execution.Initialize.impl;

import System.util.Configuration;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.table.RecordSchema;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.LongDataBox;
import engine.table.datatype.DataBoxImpl.StringDataBox;
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

import static UserApplications.CONTROL.*;
import static UserApplications.constants.StreamLedgerConstants.Constant.ACCOUNT_ID_PREFIX;
import static UserApplications.constants.StreamLedgerConstants.Constant.BOOK_ENTRY_ID_PREFIX;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;
import static utils.PartitionHelper.getPartition_interval;

public class LSInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(LSInitializer.class);
    protected int partition_interval;
    public LSInitializer(Database db, double scale_factor, double theta, int partition_num, Configuration config) {
        super(db, scale_factor, theta, partition_num, config);
        partition_interval = (int) Math.ceil(NUM_ITEMS / (double) partition_num);//NUM_ITEMS / tthread;
    }
    @Override
    public void creates_Table(Configuration config) {
        if (enable_states_partition){
            for (int i=0;i<partition_num;i++){
                RecordSchema s = AccountsScheme();
                db.createTable(s, "accounts_"+i, TransactionalProcessConstants.DataBoxTypes.LONG);
                RecordSchema b = BookEntryScheme();
                db.createTable(b, "bookEntries_"+1,TransactionalProcessConstants.DataBoxTypes.LONG);
            }
        }else {
            RecordSchema s = AccountsScheme();
            db.createTable(s, "accounts", TransactionalProcessConstants.DataBoxTypes.LONG);
            RecordSchema b = BookEntryScheme();
            db.createTable(b, "bookEntries",TransactionalProcessConstants.DataBoxTypes.LONG);
        }
    }
    private RecordSchema getRecordSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new LongDataBox());

        fieldNames.add("Key");//PK
        fieldNames.add("Value");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema AccountsScheme() {
        return getRecordSchema();
    }


    private RecordSchema BookEntryScheme() {
        return getRecordSchema();
    }
    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAccountRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            if (enable_states_partition){
                db.InsertRecord("accounts_"+getPartitionId(key), new TableRecord(schemaRecord));
            }else{
                db.InsertRecord("accounts", new TableRecord(schemaRecord));
            }
        } catch (DatabaseException | IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAssetRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        SchemaRecord schemaRecord= new SchemaRecord(values);
        try {
            if (enable_states_partition){
                db.InsertRecord("bookEntries_"+getPartitionId(key), new TableRecord(schemaRecord));
            }else {
                db.InsertRecord("bookEntries", new TableRecord(schemaRecord));
            }
        } catch (DatabaseException | IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void loadDB(int thread_id, TopologyContext context) {
        int partition_interval=getPartition_interval();
        int left_bound=thread_id*partition_interval;
        int right_bound;
        if(thread_id==context.getNUMTasks()-1){//last executor need to handle right-over
            right_bound=NUM_ITEMS;
        }else{
            right_bound=(thread_id+1)*partition_interval;
        }
        for (int key = left_bound; key < right_bound ; key++) {
            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0);
        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }
    private String GenerateKey(String prefix, int key) {
//        return rightpad(prefix + String.valueOf(key), VALUE_LEN);
        return prefix + String.valueOf(key);
    }

    @Override
    public void reloadDB(List<Integer> rangeId) {
        for (int key = 0; key < NUM_SEGMENTS ; key++) {
            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            if(rangeId.contains(getPartitionId(_key))){
                insertAccountRecord(_key, 0);
            }
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            if(rangeId.contains(getPartitionId(_key))){
                insertAssetRecord(_key, 0);
            }
        }
    }
    private int getPartitionId(String key) {
        Integer _key = Integer.valueOf(key);
        return _key / partition_interval;
    }
}
