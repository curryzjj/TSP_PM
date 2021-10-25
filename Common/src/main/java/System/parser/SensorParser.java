package System.parser;

import System.spout.helper.parser.Parser;
import applications.DataTypes.StreamValues;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SensorParser extends Parser {
    private static final Logger LOG= LoggerFactory.getLogger(SensorParser.class);
    private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
            .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
            .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
            .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
            .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final int DATE_FIELD = 0;
    private static final int TIME_FIELD = 1;
    private static final int EPOCH_FIELD = 2;
    private static final int MOTEID_FIELD = 3;
    private static final int TEMP_FIELD = 4;
    private static final int HUMID_FIELD = 5;
    private static final int LIGHT_FIELD = 6;
    private static final int VOLT_FIELD = 7;
    @Override
    public Object parse(char[] str) {
        return null;
    }

    @Override
    public List<StreamValues> parse(String value) {
        return null;
    }
}
