package streamprocess.faulttolerance.clr;

import streamprocess.controller.output.Determinant.InsideDeterminant;
import streamprocess.controller.output.Determinant.OutsideDeterminant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class CausalService implements Serializable {
    private static final long serialVersionUID = -7550158365360469274L;
    /**
     * <Input,Determinant>
     */
    public long currentMarkerId;
    public ConcurrentHashMap<Long, InsideDeterminant> insideDeterminant;
    public List<OutsideDeterminant> outsideDeterminant;
    public List<Long> abortEvent;
    public CausalService() {
        insideDeterminant = new ConcurrentHashMap<>();
        outsideDeterminant = new ArrayList<>();
        abortEvent = new ArrayList<>();
    }
    public void addInsideDeterminant(InsideDeterminant insideDeterminant) {
        this.insideDeterminant.put(insideDeterminant.input, insideDeterminant);
    }
    public void addOutsideDeterminant(OutsideDeterminant outsideDeterminant) {
        this.outsideDeterminant.add(outsideDeterminant);
    }
    public void addAbortEvent(long input) {
        this.abortEvent.add(input);
    }
    public void cleanDeterminant() {
        this.insideDeterminant.clear();
        this.outsideDeterminant.clear();
    }

    public void setCurrentMarkerId(long currentMarkerId) {
        this.currentMarkerId = currentMarkerId;
    }
}