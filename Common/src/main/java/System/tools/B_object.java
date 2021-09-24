package System.tools;

import java.util.ArrayList;

public class B_object extends object{
    byte value;
    public B_object(){
        value=0;
    }
    @Override
    public ArrayList<object> create_myObjectList(int size_state) {
        ArrayList<object> myObjectList=new ArrayList<>();
        for(int i=0;i<size_state;i++){
            myObjectList.add(new B_object());
        }
        return myObjectList;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
