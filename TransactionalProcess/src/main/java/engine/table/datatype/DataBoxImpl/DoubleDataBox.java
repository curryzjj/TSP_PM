package engine.table.datatype.DataBoxImpl;

import engine.Exception.DataBoxException;
import engine.table.datatype.DataBox;
import engine.table.datatype.serialize.Serialize;
import utils.TransactionalProcessConstants.DataBoxTypes;

import java.io.IOException;

/**
 * Float data type which serializes to 14 bytes.
 */
public class DoubleDataBox extends DataBox {
    private volatile double d;
    public DoubleDataBox(){
        this.d=0.0d;
    }
    public DoubleDataBox(double f) {
        this.d = f;
    }

    @Override
    public int getSize() throws DataBoxException {
        return 8;
    }

    @Override
    public DoubleDataBox clone() {
        return new DoubleDataBox(d);
    }

    @Override
    public double getDouble() {
        return this.d;
    }

    @Override
    public void setDouble(double f) {
        this.d=f;
    }

    @Override
    public DataBoxTypes type() throws DataBoxException {
        return DataBoxTypes.FLOAT;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (this == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        DoubleDataBox other = (DoubleDataBox) obj;
        return this.getFloat() == other.getFloat();
    }

    @Override
    public int hashCode() {
       return (int) this.getFloat();
    }

    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataBoxException("Invalid Comparsion");
        }
        DoubleDataBox other = (DoubleDataBox) obj;
        return Double.compare(this.getDouble(), other.getDouble());
    }

    @Override
    public byte[] Serialize() throws DataBoxException, IOException {
        return Serialize.serializeObject(d);
    }
}
