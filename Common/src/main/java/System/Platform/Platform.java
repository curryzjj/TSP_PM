package System.Platform;

import java.util.ArrayList;
import java.util.Arrays;

public class Platform {

    public static ArrayList[] getNodes(int machine){
        ArrayList<Integer> node_0;
        if(machine==0){//mac
            Integer[] no_0 = {5, 4, 3, 2, 1, 0};
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            return new ArrayList[]{
                    node_0
            };
        }else{
            throw new UnsupportedOperationException("unsupported machine");
        }
    }
}
