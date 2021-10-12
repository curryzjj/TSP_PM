import java.util.*;

public class Test {
    private List<String> _fields;
    public Test(String... fields) {
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
    public List<Object> select(Test input_fields,Object... tuple){
        List<Object> ret=new ArrayList<>(input_fields._fields.size());
        for(String s:input_fields._fields){
            ret.add(tuple[_index.get(s)]);
        }
        return ret;
    }
    public static void main(String[] args){
        Child child=new Child();
        Child child1=new Child("1");
    }
}
