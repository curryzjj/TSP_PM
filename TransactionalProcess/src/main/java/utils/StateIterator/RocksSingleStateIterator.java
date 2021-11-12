package utils.StateIterator;

import System.util.IOUtil;

import javax.annotation.Nonnull;

public class RocksSingleStateIterator implements SingleStateIterator{
    /**
     * @param iterator underlying {@link RocksIteratorWrapper}
     * @param kvStateId Id of the K/V state to which this iterator belongs.
     */
    RocksSingleStateIterator(@Nonnull RocksIteratorWrapper iterator, int kvStateId) {
        this.iterator = iterator;
        this.currentKey = iterator.key();
        this.kvStateId = kvStateId;
    }

    @Nonnull private final RocksIteratorWrapper iterator;
    private byte[] currentKey;
    private final int kvStateId;

    @Override
    public void next() {
        iterator.next();
        if (iterator.isValid()) {
            currentKey = iterator.key();
        }
    }

    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return iterator.value();
    }

    @Override
    public int getKvStateId() {
        return kvStateId;
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(iterator);
    }
}
