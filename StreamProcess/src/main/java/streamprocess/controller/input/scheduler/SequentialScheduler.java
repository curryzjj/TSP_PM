package streamprocess.controller.input.scheduler;

import streamprocess.controller.input.InputStreamController;
import streamprocess.execution.runtime.tuple.JumboTuple;
import streamprocess.execution.runtime.tuple.Tuple;
import streamprocess.optimization.model.STAT;

public class SequentialScheduler extends InputStreamController {
    @Override
    public JumboTuple fetchResults_inorder() {
        return null;
    }

    @Override
    public Object fetchResults() {
        return null;
    }

    @Override
    public Tuple fetchResults_single() {
        return null;
    }

    @Override
    public JumboTuple fetchResults(STAT stat, int batch) {
        return null;
    }
}
