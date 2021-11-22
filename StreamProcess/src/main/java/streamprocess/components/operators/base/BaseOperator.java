package streamprocess.components.operators.base;

import org.slf4j.Logger;
import streamprocess.components.operators.api.AbstractBolt;

import java.util.Map;

public abstract class BaseOperator extends AbstractBolt {
    protected BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                           boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, byP, event_frequency, w);
    }
    protected BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                           double branch_selectivity, double read_selectivity, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, event_frequency, w);
    }
    public BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity) {
        super(log, input_selectivity, output_selectivity, false, 0, 1);
    }
    protected BaseOperator(Logger log) {
        super(log, null,
                null, false, 0, 1);
    }
    protected BaseOperator(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity,
                        double branch_selectivity, double read_selectivity) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, 0, 0);
    }
}
