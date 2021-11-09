package engine.table.datatype.DataBoxImpl;

import engine.Exception.DataBoxException;
import engine.table.datatype.DataBox;
import engine.table.datatype.serialize.Serialize;
import utils.TransactionalProcessConstants.DataBoxTypes;


import java.io.IOException;
import java.util.HashSet;

public class HashSetDataBox extends DataBox {
    private volatile HashSet set;

    /**
     * Construct an empty IntDataBox with value_list 0.
     */
    public HashSetDataBox() {
        this.set = new HashSet();
    }

    public HashSetDataBox(HashSet set) {
        this.set = set;
    }


    @Override
    public HashSetDataBox clone() {
        return new HashSetDataBox(set);
    }

    @Override
    public DataBoxTypes type() {
        return DataBoxTypes.OTHERS;
    }

    public HashSet getHashSet() {
        return set;
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
        HashSetDataBox other = (HashSetDataBox) obj;
        return this.getInt() == other.getInt();
    }

    @Override
    public int hashCode() {
        return Math.abs(this.getInt());
    }

    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataBoxException("Invalid Comparsion");
        }
        HashSetDataBox other = (HashSetDataBox) obj;
        return Integer.compare(this.getInt(), other.getInt());
    }

    @Override
    public byte[] getBytes() {
        //not applicable.
        return null;
    }

    @Override
    public byte[] Serialize() throws DataBoxException, IOException {
        return Serialize.serializeObject(set);
    }

    @Override
    public int getSize() {
        return -1;
    }

    @Override
    public String toString() {
        return "" + this.set;
    }
}
