package applications.events.lr;

import applications.events.TxnParam;

public class LRParam extends TxnParam {
    int[] segmentIds;
    public LRParam(int numRoads) {
        segmentIds = new int[numRoads];
    }
    public String segmentId(int i) {
        return String.valueOf(segmentIds[i]);
    }
    public int[] getSegmentIds() {
        return segmentIds;
    }

    @Override
    public void set_keys(int access_id, int res) {
        this.segmentIds[access_id] = res;
    }
}
