package applications.DataTypes;

import applications.DataTypes.util.Constants;
import applications.DataTypes.util.IPositionIdentifier;
import applications.DataTypes.util.ISegmentIdentifier;
import applications.DataTypes.util.LRTopologyControl;
import streamprocess.execution.runtime.tuple.Fields;

public class PositionReport extends AbstractInputTuple implements IPositionIdentifier, ISegmentIdentifier {
    /** The index of the speed attribute. */
    public final static int SPD_IDX = 3;

    /** The index of the express way attribute. */
    public final static int XWAY_IDX = 4;

    /** The index of the lane attribute. */
    public final static int LANE_IDX = 5;

    /** The index of the direction attribute. */
    public final static int DIR_IDX = 6;

    /** The index of the segment attribute. */
    public final static int SEG_IDX = 7;

    /** The index of the position attribute. */
    public final static int POS_IDX = 8;

    public PositionReport() {
        super();
    }//nothing
    /**
     * Instantiates a new position record for the given attributes.
     *
     * @param time
     *            the time at which the position record was emitted (in LRB seconds)
     * @param vid
     *            the vehicle identifier
     * @param speed
     *            the current speed of the vehicle
     * @param xway
     *            the current expressway
     * @param lane
     *            the lane of the expressway
     * @param direction
     *            the traveling direction
     * @param segment
     *            the mile-long segment of the highway
     * @param position
     *            the horizontal position on the expressway
     */
    public PositionReport(Short time, Integer vid, Integer speed, Integer xway, Short lane, Short direction,
                          Short segment, Integer position) {
        super(AbstractLRBTuple.POSITION_REPORT, time, vid);

        assert (speed != null);
        assert (xway != null);
        assert (lane != null);
        assert (direction != null);
        assert (segment != null);
        assert (position != null);

        super.add(SPD_IDX, speed);
        super.add(XWAY_IDX, xway);
        super.add(LANE_IDX, lane);
        super.add(DIR_IDX, direction);
        super.add(SEG_IDX, segment);
        super.add(POS_IDX, position);

        assert (super.size() == 9);
    }
    /**
     * Returns the vehicle's speed of this {@link PositionReport}.
     *
     * @return the speed of this position report
     */
    public final Integer getSpeed() {
        return (Integer)super.get(SPD_IDX);
    }

    /**
     * Returns the expressway ID of this {@link PositionReport}.
     *
     * @return the VID of this position report
     */
    @Override
    public final Integer getXWay() {
        return (Integer)super.get(XWAY_IDX);
    }

    /**
     * Returns the lane of this {@link PositionReport}.
     *
     * @return the VID of this position report
     */
    @Override
    public final Short getLane() {
        return (Short)super.get(LANE_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link PositionReport}.
     *
     * @return the VID of this position report
     */
    @Override
    public final Short getDirection() {
        return (Short)super.get(DIR_IDX);
    }

    /**
     * Returns the segment of this {@link PositionReport}.
     *
     * @return the VID of this position report
     */
    @Override
    public final Short getSegment() {
        return (Short)super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's position of this {@link PositionReport}.
     *
     * @return the VID of this position report
     */
    @Override
    public final Integer getPosition() {
        return (Integer)super.get(POS_IDX);
    }

    /**
     * Checks if the vehicle is on the exit lane or not.
     *
     * @return {@code true} if the vehicle is on the exit lane -- {@code false} otherwise
     */
    public boolean isOnExitLane() {
        return this.getLane().shortValue() == Constants.EXIT_LANE;
    }

    /**
     * Return a copy of this {@link PositionReport}.
     *
     * @return a copy of this {@link PositionReport}
     */
    public PositionReport copy() {
        PositionReport pr = new PositionReport();
        pr.addAll(this);
        return pr;
    }

    /**
     * Returns the schema of a {@link PositionReport}.
     *
     * @return the schema of a {@link PositionReport}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.TYPE_FIELD_NAME, LRTopologyControl.TIMESTAMP_FIELD_NAME,
                LRTopologyControl.VEHICLE_ID_FIELD_NAME, LRTopologyControl.SPEED_FIELD_NAME, LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.LANE_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME, LRTopologyControl.SEGMENT_FIELD_NAME,
                LRTopologyControl.POSITION_FIELD_NAME);
    }

    public static Fields getLatencySchema() {
        return null;
    }

}
