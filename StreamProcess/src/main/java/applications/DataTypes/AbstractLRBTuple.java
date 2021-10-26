package applications.DataTypes;

import System.util.DataTypes.StreamValues;

public abstract class AbstractLRBTuple extends StreamValues {
    /**
     * The tuple type ID for position reports.
     */
    public final static short position_report = 0;
    /**
     * The tuple type ID for account balance requests.
     */
    public final static short account_balance_request = 2;
    /**
     * The tuple type ID for account balance requests as object (see {@link #account_balance_request}).
     */
    @SuppressWarnings("boxing")
    public final static Short ACCOUNT_BALANCE_REQUEST = account_balance_request;
    /**
     * The tuple type ID for daily expenditure requests.
     */
    public final static short daily_expenditure_request = 3;
    /**
     * The tuple type ID for daily expenditure requests as object (see {@link #daily_expenditure_request}).
     */
    @SuppressWarnings("boxing")
    public final static Short DAILY_EXPENDITURE_REQUEST = daily_expenditure_request;
    /**
     * The tuple type ID for travel time requests.
     */
    public final static short travel_time_request = 4;
    /**
     * The index of the TIME attribute.
     */
    public final static int TIME_IDX = 1;
    /**
     * The tuple type ID for position reports as object (see {@link #position_report}).
     */
    @SuppressWarnings("boxing")
    final static Short POSITION_REPORT = position_report;
    /**
     * The tuple type ID for travel time requests as object (see {@link #travel_time_request}).
     */
    @SuppressWarnings("boxing")
    final static Short TRAVEL_TIME_REQUEST = travel_time_request;
    /**
     * The tuple type ID for toll notifications.
     */
    private final static short toll_notification = 0;
    /**
     * The tuple type ID for toll notifications as object (see {@link #toll_notification}).
     */
    @SuppressWarnings("boxing")
    final static Short TOLL_NOTIFICATION = toll_notification;
    /**
     * The tuple type ID for accident notifications.
     */
    private final static short accident_notification = 1;
    /**
     * The tuple type ID for accident notifications as object (see {@link #accident_notification}).
     */
    @SuppressWarnings("boxing")
    final static Short ACCIDENT_NOTIFICATION = accident_notification;
    /**
     * The index of the TYPE attribute.
     */
    private final static int TYPE_IDX = 0;
    AbstractLRBTuple() {
    }

    AbstractLRBTuple(Short type, Short time) {
        assert (type != null);
        assert (time != null);
        assert (type == position_report || type == account_balance_request
                || type == daily_expenditure_request || type == travel_time_request
                || type == toll_notification || type == accident_notification);
        assert (time.shortValue() >= 0);

        super.add(TYPE_IDX, type);
        super.add(TIME_IDX, time);

        assert (super.size() == 2);
    }


    /**
     * Returns the tuple type ID of this {@link AbstractLRBTuple}.
     *
     * @return the type ID of this tuple
     */
    final Short getType() {
        return (Short) super.get(TYPE_IDX);
    }

    /**
     * Returns the timestamp (in LRB seconds) of this {@link AbstractLRBTuple}.
     *
     * @return the timestamp of this tuple
     */
    public final Short getTime() {
        return (Short)super.get(TIME_IDX);
    }
    /**
     * TODO remove class Time ???
     *
     * @return
     */
    public final short getMinuteNumber() {
        short timestamp=this.getTime().shortValue();
        assert (timestamp>=0);
        return (short)((timestamp/60)+1);
    }
}
