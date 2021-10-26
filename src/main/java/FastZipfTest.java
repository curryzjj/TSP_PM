import System.tools.FastZipfGenerator;

import java.util.*;

public class FastZipfTest {
    private List<String> _fields;
    public FastZipfTest(String... fields) {
        this.Fields(Arrays.asList(fields));
        System.out.println(Arrays.asList(fields));
    }
    private final Map<String, Integer> _index = new HashMap<>();
    public void Fields(List<String> fields) {
        _fields = new ArrayList<>(fields.size());
        for (String field : fields) {
            if (_fields.contains(field)) {
                throw new IllegalArgumentException(
                        String.format("duplicate field '%s'", field)
                );
            }
            _fields.add(field);
        }
        index();
    }
    private void index() {
        for (int i = 0; i < _fields.size(); i++) {
            _index.put(_fields.get(i), i);
        }
    }
    public List<Object> select(FastZipfTest input_fields, Object... tuple){
        List<Object> ret=new ArrayList<>(input_fields._fields.size());
        for(String s:input_fields._fields){
            ret.add(tuple[_index.get(s)]);
        }
        return ret;
    }
    public static void main(String[] args) throws InterruptedException {
        FastZipfGenerator keygenerator=new FastZipfGenerator(40,1,0);
        Set[] input_keys = new Set[10_000];
        for (int k = 0; k < 10_000; k++) {
            Set<Integer> keys = new LinkedHashSet<>();
            for (int i = 0; i < 30; i++) {
                int key = keygenerator.next();
                while (keys.contains(key)) {
                    key = keygenerator.next();
                }
                keys.add(key);
            }
            input_keys[k] = keys;
        }
        Set<Integer> keys = new LinkedHashSet<>();
        keys.addAll(input_keys[0 % 10_000]);
        System.out.println(keys);
        long[] p_bid=new long[30];
        for (int i=0;i<30;i++){//what is this for?
            p_bid[i]=0;
        }
    }
}
