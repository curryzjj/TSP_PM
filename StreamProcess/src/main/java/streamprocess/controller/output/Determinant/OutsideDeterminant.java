package streamprocess.controller.output.Determinant;

import applications.events.TxnEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Add to the operation_chain when recovery
 */
public class OutsideDeterminant implements Serializable {
    private static final long serialVersionUID = 1697109885782459412L;
    public TxnEvent outSideEvent;
    public List<Integer> targetIds = new ArrayList<>();
    public void setOutSideEvent(TxnEvent outSideEvent) {
        this.outSideEvent = outSideEvent;
    }
    public void setTargetId(int targetIds) {
        if (this.targetIds.contains(targetIds)){
            return;
        }
        this.targetIds.add(targetIds);
    }
}
