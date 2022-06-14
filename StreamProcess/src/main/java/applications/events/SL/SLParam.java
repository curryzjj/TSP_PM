package applications.events.SL;

import applications.events.TxnParam;

public class SLParam extends TxnParam {
    int[] userIds;
    public SLParam(int numItems) {
        userIds = new int[numItems];
    }
    @Override
    public void set_keys(int access_id, int res) {
        userIds[access_id] = res;
    }

    public int[] keys() {
        return userIds;
    }
}
