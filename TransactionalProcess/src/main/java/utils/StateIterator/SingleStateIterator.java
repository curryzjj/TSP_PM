package utils.StateIterator;

import java.io.Closeable;

public interface SingleStateIterator extends Closeable {
    void next();

    boolean isValid();

    byte[] key();

    byte[] value();

    int getKvStateId();

    @Override
    void close();
}
