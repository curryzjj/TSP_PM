package System.spout.helper.wrapper.basic;

import System.spout.helper.Event;
import System.tools.B_object;
import System.tools.object;
import System.util.Configuration;

import java.io.Serializable;
import java.util.ArrayList;

public class StateWrapper<T> implements Serializable {
    protected Configuration config;
    private String tuple_states=null;
    private String flag;//not sure
    public StateWrapper(boolean verbose,int size){
        this(size);
        flag="|"+ Event.split_expression;
    }

    public StateWrapper(int size) {
        flag=null;
    }
    public String getFlag(int index_e){
        if(index_e % 500==0){//monitor per 100 event to reduce overhead.
            return flag;
        }else{
            return Event.null_expression;
        }
    }
    private void create_states(int size){
        if(size==0){
            return;
        }
        ArrayList<object> list;
        list=new B_object().create_myObjectList(size);
        StringBuilder stringBuilder=new StringBuilder();
        for(object obj : list){
            stringBuilder.append(obj.getValue());
        }
        tuple_states=stringBuilder.toString();//wrap all the states into one string
    }
    public String getTuple_states() {
        return tuple_states;
    }
}
