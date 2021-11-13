package applications.DataTypes;

public abstract class AbstractInputTuple extends AbstractLRBTuple {
    // attribute indexes
    /** The index of the VID attribute. */
    public final static int VID_IDX = 2;
    protected AbstractInputTuple() {
        super();
    }
    protected AbstractInputTuple(Short type, Long time, Integer vid) {
        super(type, time);
        assert (vid != null);
        super.add(VID_IDX, vid);
        assert (super.size() == 3);
    }
    /**
     * Returns the vehicle ID of this {@link AbstractInputTuple}.
     *
     * @return the VID of this tuple
     */
    public final Integer getVid() {
        return (Integer)super.get(VID_IDX);
    }
}
