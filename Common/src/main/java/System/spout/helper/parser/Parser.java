package System.spout.helper.parser;

import System.util.Configuration;
import System.util.DataTypes.StreamValues;

import java.io.Serializable;
import java.util.List;

public abstract class Parser<T> implements Serializable {
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }

    public abstract T parse(char[] str);

    public abstract List<StreamValues> parse(String value);
}
