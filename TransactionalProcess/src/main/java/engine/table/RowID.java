package engine.table;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents the ID of a partition d_record. Stores the id of a page and the slot number select this
 * d_record lives within that page.
 */
public class RowID {
    private int id;
    public RowID(int id){
        this.id=id;
    }
    public RowID(byte[] buffer){
        ByteBuffer byteBuffer=ByteBuffer.wrap(buffer);
        this.id=byteBuffer.getInt();
    }

    @Override
    public String toString() {
        return "RowID{" +
                "id=" + id +
                '}';
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RowID)) {
            return false;
        }
        RowID otherRecord = (RowID) other;
        return otherRecord.getID() == this.getID();
    }
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    public byte[] getBytes() {
        return ByteBuffer.allocate(8).putLong(getID()).array();
    }

    public int compareTo(Object obj) {
        RowID other = (RowID) obj;
        return Long.compare(this.getID(), other.getID());
    }
    static public int getSize(){
        return 8;
    }
    public int getID() {
        return id;
    }
}
