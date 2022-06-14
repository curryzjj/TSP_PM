package utils.StateIterator.ImplSingleStateIterator;

import engine.table.datatype.serialize.Serialize;
import engine.table.tableRecords.TableRecord;
import utils.StateIterator.InMemoryTableIteratorWrapper;
import utils.StateIterator.SingleStateIterator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TableSingleStateIterator implements SingleStateIterator {
    @Nonnull private final InMemoryTableIteratorWrapper iterator;
    private final int kvStateId;
    private boolean Valid;
    public TableSingleStateIterator(@Nonnull InMemoryTableIteratorWrapper inMemoryTableIteratorWrapper,int kvStateId){
        this.iterator=inMemoryTableIteratorWrapper;
        this.kvStateId=kvStateId;
    }
    @Override
    public void next() {

    }

    @Override
    public boolean isValid() {
        return Valid ;
    }

    @Override
    public byte[] key() {
        return iterator.keyNext().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] value() {
        try {
            TableRecord tableRecord = iterator.next();
            tableRecord.clean_map();
            return Serialize.serializeObject(tableRecord);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public int getKvStateId() {
        return kvStateId;
    }

    @Override
    public void close() {

    }
    public boolean hasNext(){
        return iterator.hasNext();
    }
}
