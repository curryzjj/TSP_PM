package UserApplications.constants;

import System.constants.BaseConstants;

public interface WordCountConstants extends BaseConstants {
    interface Component extends BaseComponent {
        String SPLITTER = "splitSentence";
        String COUNTER = "wordCount";
        String AGG = "aggregator";
    }
}
