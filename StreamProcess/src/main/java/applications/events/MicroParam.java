package applications.events;

public class MicroParam extends TxnParam{
    /* It should be arraylist instead of linkedlist as there is no add/remove later */
    int[] keys;
    public MicroParam(int numItems){
        keys=new int[numItems];
    }
    public String keys(int i){
        return String.valueOf(keys[i]);
    }

    public int[] getKeys() {
        return keys;
    }

    @Override
    public void set_keys(int access_id, int _key) {
        this.keys[access_id]=_key;
    }
}
