package streamprocess.execution.runtime.tuple;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

public class Fields implements Iterable<String>, Serializable {
    private final List<String> _fields;
    private final Map<String,Integer> _index=new HashMap<>();
    public Fields(String... fields){this(Arrays.asList(fields));}
    public Fields(List<String> fields) {
        _fields = new ArrayList<>(fields.size());
        for(String field:fields){
            if(_fields.contains(fields)){
                throw new IllegalArgumentException(String.format("duplicate field '%s'",field));
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
    @Override
    public Iterator<String> iterator() {
        return null;
    }

    @Override
    public void forEach(Consumer<? super String> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<String> spliterator() {
        return Iterable.super.spliterator();
    }
}
