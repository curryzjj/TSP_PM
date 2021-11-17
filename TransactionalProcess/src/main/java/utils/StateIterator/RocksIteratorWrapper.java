package utils.StateIterator;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RocksIteratorWrapper implements RocksIteratorInterface, Closeable {
    private RocksIterator iterator;

    public RocksIteratorWrapper(@Nonnull RocksIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean isValid() {
        return this.iterator.isValid();
    }

    @Override
    public void seekToFirst() {
        iterator.seekToFirst();
        status();
    }

    @Override
    public void seekToLast() {
        iterator.seekToLast();
        status();
    }

    @Override
    public void seek(byte[] target) {
        iterator.seek(target);
        status();
    }

    @Override
    public void seekForPrev(byte[] target) {
        iterator.seekForPrev(target);
        status();
    }

    @Override
    public void seek(ByteBuffer byteBuffer) {
        iterator.seek(byteBuffer);
    }

    @Override
    public void seekForPrev(ByteBuffer byteBuffer) {
        iterator.seekForPrev(byteBuffer);
    }

    @Override
    public void next() {
        iterator.next();
        status();
    }

    @Override
    public void prev() {
        iterator.prev();
        status();
    }

    @Override
    public void status() {
        try {
            iterator.status();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void refresh() throws RocksDBException {
        iterator.refresh();
    }

    public byte[] key() {
        return iterator.key();
    }

    public byte[] value() {
        return iterator.value();
    }

    @Override
    public void close() {
        iterator.close();
    }
}
