package streamprocess.execution.Initialize.impl;

import System.util.Configuration;
import engine.Database;
import engine.table.RecordSchema;
import engine.table.datatype.DataBox;
import engine.table.datatype.DataBoxImpl.DoubleDataBox;
import engine.table.datatype.DataBoxImpl.HashSetDataBox;
import engine.table.datatype.DataBoxImpl.StringDataBox;
import streamprocess.execution.Initialize.TableInitilizer;

import java.util.ArrayList;
import java.util.List;

public class TPInitializer extends TableInitilizer {
    public TPInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
    }

    @Override
    public void creates_Table(Configuration config) {
        RecordSchema s = SpeedScheme();
        db.createTable(s, "segment_speed");

        RecordSchema b = CntScheme();
        db.createTable(b, "segment_cnt");
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
