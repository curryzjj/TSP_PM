package utils.StateIterator;

import java.io.IOException;

public interface KeyValueStateIterator extends AutoCloseable {
    /**
     * Advances the iterator. Should only be called if {@link #isValid()} returned true. Valid flag
     * can only change after calling {@link #next()}.
     */
    void next() throws IOException;

    /** Returns the key-group for the current key. */
    int keyGroup();

    byte[] key();

    byte[] value();

    /** Returns the Id of the K/V state to which the current key belongs. */
    int kvStateId();

    /**
     * Indicates if current key starts a new k/v-state, i.e. belong to a different k/v-state than
     * it's predecessor.
     *
     * @return true iff the current key belong to a different k/v-state than it's predecessor.
     */
    boolean isNewKeyValueState();

    /**
     * Indicates if current key starts a new key-group, i.e. belong to a different key-group than
     * it's predecessor.
     *
     * @return true iff the current key belong to a different key-group than it's predecessor.
     */
    boolean isNewKeyGroup();

    /**
     * Check if the iterator is still valid. Getters like {@link #key()}, {@link #value()}, etc. as
     * well as {@link #next()} should only be called if valid returned true. Should be checked after
     * each call to {@link #next()} before accessing iterator state.
     *
     * @return True iff this iterator is valid.
     */
    boolean isValid();

    @Override
    void close();
}
