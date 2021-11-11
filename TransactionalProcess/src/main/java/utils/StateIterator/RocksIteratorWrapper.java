package utils.StateIterator;

import org.apache.flink.util.FlinkRuntimeException;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RocksIteratorWrapper implements RocksIteratorInterface, Closeable {
    private RocksIterator iterator;
    public RocksIteratorWrapper(@Nonnull RocksIterator iterator){
        this.iterator=iterator;
    }

    @Override
    public boolean isValid() {
        return this.iterator.isValid();
    }

    @Override
    public void seekToFirst() {
        iterator.seekToFirst();
        try {
            status();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void seekToLast() {
        iterator.seekToFirst();
        try {
            status();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void seek(byte[] target) {
        iterator.seek(target);
        try {
            status();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void seekForPrev(byte[] target) {
        iterator.seekForPrev(target);
        try {
            status();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void seek(ByteBuffer byteBuffer) {

    }

    @Override
    public void seekForPrev(ByteBuffer byteBuffer) {

    }

    @Override
    public void next() {
        iterator.next();
        try {
            status();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void prev() {
        iterator.prev();
        try {
            status();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void status() throws RocksDBException {
        try {
            iterator.status();
        } catch (RocksDBException ex) {
            throw new FlinkRuntimeException("Internal exception found in RocksDB", ex);
        }
    }

    @Override
    public void refresh() throws RocksDBException {

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
