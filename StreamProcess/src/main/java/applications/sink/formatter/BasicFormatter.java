package applications.sink.formatter;

import streamprocess.execution.runtime.tuple.Fields;
import streamprocess.execution.runtime.tuple.Tuple;

public class BasicFormatter extends Formatter{
    public String format(Tuple tuple) {
        Fields schema = context.getComponentOutputFields(tuple.getSourceComponent(), tuple.getSourceStreamId());

        StringBuilder line = new StringBuilder();

        for (int i = 0; i < tuple.fieldSize(); i++) {
            if (i != 0) {
                line.append(", ");
            }
            line.append(String.format("%s=%s", schema.get(i), tuple.getValue(i)));
        }

        return line.toString();
    }
}
