package UserApplications.constants;

import System.constants.BaseConstants;

public interface WordCountConstants extends BaseConstants {
    String PREFIX="wc";
    interface Field extends BaseField{
        String WORD = "word";
        String COUNT = "count";
        String LargeData = "LD";
    }
    interface Component extends BaseComponent {
        String SPLITTER = "splitSentence";
        String COUNTER = "wordCount";
        String AGG = "aggregator";
    }
}
