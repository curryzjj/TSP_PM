package utils.StateIterator;

public interface TableStateIterator extends kvStateIterator{
    /**
     * Advances the iterator. Should only be called if {@link #isValid()} returned true. Valid flag
     * can only change after calling {@link #next()}.
     */
    /** Returns the Id of the K/V state to which the current key belongs. */
    int kvStateId();
    /**
     * Indicates if current key starts a new k/v-state, i.e. belong to a different k/v-state than
     * it's predecessor.
     *
     * @return true iff the current key belong to a different k/v-state than it's predecessor.
     */
    boolean isNewKeyValueState();
    boolean isValid();
    boolean isIteratorValid();
    void close();
    void switchIterator();

    byte[] nextkey();

    byte[] nextvalue();
}
