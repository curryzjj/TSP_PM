package engine.table.datatype;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class TimestampType implements Comparable<TimestampType>, Serializable {
    public static final String STRING_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private final Date m_date;     // stores milliseconds from epoch.
    private final short m_usecs;   // stores microsecond within date's millisecond.
    /**
     * Create a TimestampType from microseconds from epoch.
     *
     * @param timestamp microseconds since epoch.
     */
    public TimestampType(long timestamp) {
        m_usecs = (short) (timestamp % 1000);
        long millis = (timestamp - m_usecs) / 1000;
        m_date = new Date(millis);
    }
    /**
     * Create a new TimestampType from a given date object
     *
     * @param date
     */
    public TimestampType(Date date) {
        m_usecs = 0;
        m_date = date;
    }
    /**
     * Create a TimestampType instance for the current time.
     */
    public TimestampType() {
        this(new Date());
    }
    /**
     * Read the microsecond in time stored by this timestamp.
     * @return microseconds
     */
    public long getTime() {
        long millis = m_date.getTime();
        return millis * 1000 + m_usecs;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TimestampType)) {
            return false;
        }
        TimestampType ts = (TimestampType) o;
        return ts.m_date.equals(this.m_date) && ts.m_usecs == this.m_usecs;
    }
    @Override
    public int hashCode() {
        long usec = this.getTime();
        return (int) usec ^ (int) (usec >> 32);
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat(STRING_FORMAT);
        String format = sdf.format(m_date);
        return format + "." + m_usecs;
    }

    @Override
    public int compareTo(@NotNull TimestampType o) {
        int comp = m_date.compareTo(o.m_date);
        if (comp == 0) {
            return m_usecs - o.m_usecs;
        } else {
            return comp;
        }
    }
}
