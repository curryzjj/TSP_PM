package System.parser;

import System.spout.helper.parser.Parser;
import applications.DataTypes.StreamValues;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class StringParser extends Parser<char[]> {
    private static final Logger LOG= LoggerFactory.getLogger(StringParser.class);

    @Override
    public char[] parse(char[] str) {
        if (str.length<=0){
            return null;
        }
        return Arrays.copyOf(str,str.length);
    }

    @Override
    public List<StreamValues> parse(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }

        return ImmutableList.of(new StreamValues(str));
    }
}
