package System.spout.helper;

import java.util.LinkedList;

import static System.Constants.Default_null_expression;
import static System.Constants.Default_split_expression;

public class Event extends LinkedList<String> {
    public static final String split_expression=Default_split_expression;
    public static final String null_expression=Default_null_expression;
    public Event(){};
    public Event(Long timeStamp,String key,String value){
        String sb=String.valueOf(timeStamp)+split_expression+key+split_expression+value;
        this.add(String.valueOf(timeStamp));
        this.add(key);
        this.add(value);
        this.add(sb);
    }
    public Event(Long timeStamp, String key, String value, String flag) {
        String sb = String.valueOf(timeStamp) +
                split_expression +
                key +
                split_expression +
                value +
                split_expression +
                flag;
        this.add(String.valueOf(timeStamp));
        this.add(key);
        this.add(value);
        this.add(sb);
        this.add(flag);
    }
    public String getEvent() {
        return this.get(3);
    }

    public String getKey() {
        return this.get(2);
    }
}
