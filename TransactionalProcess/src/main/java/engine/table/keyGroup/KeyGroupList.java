package engine.table.keyGroup;

public interface KeyGroupList extends Iterable<Integer>{
    /** Returns the number of key group ids in the list. */
    int getNumberOfKeyGroups();

    /**
     * Returns the id of the keygroup at the given index, where index in interval [0, {@link
     * #getNumberOfKeyGroups()}[.
     *
     * @param idx the index into the list
     * @return key group id at the given index
     */
    int getKeyGroupId(int idx);

    /** Returns true, if the given key group id is contained in the list, otherwise false. */
    boolean contains(int keyGroupId);
}
