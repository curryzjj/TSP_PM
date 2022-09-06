package streamprocess.faulttolerance.clr;

import System.tools.SortHelper;
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
    //markId, outsideDeterminant
    public ConcurrentHashMap<Long,List<OutsideDeterminant>> outsideDeterminantList;
    public ConcurrentHashMap<Long,List<Long>> abortEventList;
    public ConcurrentHashMap<Long,ConcurrentHashMap<Long, InsideDeterminant>> insideDeterminantList;
    public CausalService() {
        insideDeterminantList = new ConcurrentHashMap<>();
        outsideDeterminantList = new ConcurrentHashMap<>();
        abortEventList = new ConcurrentHashMap<>();
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
        this.abortEvent.clear();
        this.outsideDeterminantList.clear();
        this.abortEventList.clear();
        this.insideDeterminantList.clear();
    }

    public void setCurrentMarkerId(long currentMarkerId) {
        outsideDeterminantList.put(currentMarkerId, this.outsideDeterminant);
        abortEventList.put(currentMarkerId, this.abortEvent);
        insideDeterminantList.put(currentMarkerId, this.insideDeterminant);
        this.abortEvent = new ArrayList<>();
        this.outsideDeterminant =  new ArrayList<>();
        this.insideDeterminant = new ConcurrentHashMap<>();
        this.currentMarkerId = currentMarkerId;
    }
    public List<Long> getAbortEventsByMarkerId(long markerId){
        ArrayList<Long> keys = SortHelper.sortKey(this.abortEventList.keySet());
        for (long key:keys) {
            if (key > markerId) {
                return this.abortEventList.get(key);
            }
        }
        return new ArrayList<>();
    }
    public ConcurrentHashMap<Long, InsideDeterminant> getInsideDeterminantByMarkerId(long markerId){
        ArrayList<Long> keys = SortHelper.sortKey(this.insideDeterminantList.keySet());
        for (long key:keys) {
            if (key > markerId) {
                return this.insideDeterminantList.get(key);
            }
        }
        return new ConcurrentHashMap<>();
    }
}
