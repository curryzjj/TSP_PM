package streamprocess.components.operators.base;

import org.slf4j.Logger;
import streamprocess.components.operators.api.BaseOperator;
import streamprocess.components.operators.api.Operator;

import java.util.Map;

public abstract class MapBolt extends BaseOperator {
    protected MapBolt(Logger log, Map<String, Double> input_selectivity) {
        super(log, input_selectivity, null, false, 0, 1);
    }
    protected MapBolt(Map<String, Double> input_selectivity, Map<String, Double> output_selectivity) {
        super(null, input_selectivity, output_selectivity, 1, 1.0, 0, 1);
    }
    protected MapBolt(Logger log, double read_selectivity) {
        super(log, null, null, 1, read_selectivity, 0, 1);
    }
    protected MapBolt(Logger log) {
        super(log, null, null, false, 0, 1);
    }
    public String output_type(){
        return Operator.map;
    }
}
