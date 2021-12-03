package engine.table.datatype;

import engine.Exception.DataBoxException;
import engine.table.datatype.DataBoxImpl.IntDataBox;
import engine.table.tableRecords.SchemaRecord;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

public abstract class DataBox implements Comparable,Cloneable, Serializable {
    //multi-types constructions
    public DataBox() throws DataBoxException {
    }
    public DataBox(boolean b) throws DataBoxException {
        throw new DataBoxException("not boolean type");
    }
    public DataBox(int i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }
    public DataBox(float f) throws DataBoxException {
        throw new DataBoxException("not float type");
    }
    public DataBox(String s, int len) throws DataBoxException {
        throw new DataBoxException("not String type");
    }
    public DataBox(byte[] buf) throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }
    //get-set methods
    public boolean getBool() throws DataBoxException {
        throw new DataBoxException("not boolean type");
    }
    public void setBool(boolean b) throws DataBoxException {
        throw new DataBoxException("not boolean type");
    }
    public int getInt() throws DataBoxException {
        throw new DataBoxException("not int type");
    }
    public void setInt(int i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }
    public long getLong() throws DataBoxException {
        throw new DataBoxException("not long type");
    }
    public void setLong(long i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }
    public TimestampType getTimestamp() throws DataBoxException {
        throw new DataBoxException("not int type");
    }
    public void setTimestamp(TimestampType i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }
    public float getFloat() throws DataBoxException {
        throw new DataBoxException("not float type");
    }
    public void setFloat(float f) throws DataBoxException {
        throw new DataBoxException("not float type");
    }
    public double getDouble() {
        throw new DataBoxException("not double type");
    }
    public void setDouble(double f) {
        throw new DataBoxException("not double type");
    }
    public byte[] getBytes() throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }
    public void incLong(long current_value, long delta) {
        throw new DataBoxException("not string type");
    }
    public void incLong(SchemaRecord s_record, long delta) {
        throw new DataBoxException("not string type");
    }
    public void incLong(long delta) {
        incLong(getLong(), delta);
    }
    public void decLong(SchemaRecord s_record, long delta) {
        throw new DataBoxException("not string type");
    }
    public void decLong(long current_value, long delta) {
        throw new DataBoxException("not string type");
    }
    public double addItem(Double nextDouble) {
        throw new DataBoxException("not list type");
    }
    public List<Double> getList() {
        throw new DataBoxException("not list type");
    }
    public void setString(String s, int len) throws DataBoxException {
        throw new DataBoxException("not string type");
    }
    //clone
    @Override
    public DataBox clone() {
        throw new DataBoxException("not cloneable");
    }
    //override but not implemented
    public int getSize() throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }
    public int compareTo(Object obj) throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }
    public HashSet getHashSet() {
        throw new DataBoxException("Not Implemented");
    }
    @Override
    public String toString() throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }
    //multi types
    /**
     * All data must be able to provide a string representation for indexing purpose...
     * TODO: think about better way later.
     *
     * @return
     */
    public String getString() {
       if (this instanceof IntDataBox) {
            return String.valueOf((this).getInt());
        }
        throw new DataBoxException("Not Implemented");
    }
    /**
     * Returns the type of the DataBox.
     *
     * @return the type from the Types enum
     * @throws DataBoxException
     */
    public DataBoxTypes type() throws DataBoxException {
        throw new DataBoxException("No type");
    }
    /**
     * Return serialize byte[]
     */
    public byte[] Serialize() throws DataBoxException, IOException {
        throw new DataBoxException("No Type");
    }
}
