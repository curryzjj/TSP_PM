package streamprocess.components.operators.base;

import org.slf4j.Logger;
import streamprocess.components.operators.api.Operator;

import java.util.Map;

public abstract class filterBolt extends BaseOperator {
    protected filterBolt() {
        super(null, null, null, 0.5, 1, 0, 1);
    }

    protected filterBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity) {
        super(log, input_selectivity, output_selectivity, false, 0, 1);
    }

    protected filterBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double read_selectivity) {
        super(log, input_selectivity, output_selectivity, (double) 1, read_selectivity, 0, 1);
    }

    protected filterBolt(Logger log, Map<String, Double> output_selectivity) {
        super(log, null, output_selectivity, false, 0, 1);
    }
    public String output_type() {
        return Operator.filter;
    }
}
