package utils.StateIterator;

import engine.table.tableRecords.TableRecord;
import org.jetbrains.annotations.NotNull;
import scala.util.parsing.combinator.testing.Str;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class InMemoryTableIteratorWrapper implements Iterator<TableRecord>, Closeable {
    private Iterator<TableRecord> valueIterator;
    private Iterator<String> keyIterator;
    public InMemoryTableIteratorWrapper(@Nonnull Iterator<TableRecord> iterator, Iterator<String> keyIterator){
        this.keyIterator=keyIterator;
        this.valueIterator=iterator;
    }
    @Override
    public void close() throws IOException {
    }
    public boolean isValid(){
        return true;
    }
    @Override
    public boolean hasNext() {
       return valueIterator.hasNext();
    }

    @Override
    public TableRecord next() {
       return valueIterator.next();
    }

    public String keyNext(){
        return keyIterator.next();
    }
}
