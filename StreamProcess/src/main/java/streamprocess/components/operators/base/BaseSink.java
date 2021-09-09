package streamprocess.components.operators.base;

import org.slf4j.Logger;
import streamprocess.components.operators.api.AbstractBolt;
import streamprocess.components.operators.api.BaseOperator;

import java.util.Map;

public abstract class BaseSink extends AbstractBolt {

    BaseSink(Logger log, Map<String, Double> input_selectivity,
             Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log,input_selectivity,output_selectivity,byP,event_frequency,w);
        System.out.print("");
    }
}
