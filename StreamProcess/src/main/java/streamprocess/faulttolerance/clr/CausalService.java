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
    //markId, outsideDeterminant
    public ConcurrentHashMap<Long,List<OutsideDeterminant>> outsideDeterminantList;
    public ConcurrentHashMap<Long,List<Long>> abortEventList;
    public List<OutsideDeterminant> outsideDeterminant;
    public List<Long> abortEvent;
    public CausalService() {
        insideDeterminant = new ConcurrentHashMap<>();
        outsideDeterminantList = new ConcurrentHashMap<>();
        abortEventList = new ConcurrentHashMap<>();
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
        this.outsideDeterminantList.clear();
        this.abortEvent.clear();
    }

    public void setCurrentMarkerId(long currentMarkerId) {
        outsideDeterminantList.put(currentMarkerId, this.outsideDeterminant);
        abortEventList.put(currentMarkerId,this.abortEvent);
        this.abortEvent = new ArrayList<>();
        this.outsideDeterminant =  new ArrayList<>();
        this.currentMarkerId = currentMarkerId;
    }
}
