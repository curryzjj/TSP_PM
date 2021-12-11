package System.Platform;

import java.util.ArrayList;
import java.util.Arrays;

public class Platform {

    public static ArrayList[] getNodes(int machine){
        ArrayList<Integer> node_0;
        if(machine==0){//mac
            Integer[] no_0 = {6,5,4,3,2,1};
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            return new ArrayList[]{
                    node_0
            };
        }else if(machine==1){//node22
            Integer[] no_0 = {19, 18, 17, 16, 15, 14,13,12,11,10,9,8,7,6,5,4,3,2,1,0};
            node_0 = new ArrayList<>(Arrays.asList(no_0));
            Integer[] no_1 = {39, 38, 37, 36, 35, 34,33,32,31,30,29,28,27,26,25,24,23,22,21,20};
            ArrayList<Integer>node_1 = new ArrayList<>(Arrays.asList(no_1));
            return new ArrayList[]{node_0,
                    node_1};
        }else {
            throw new UnsupportedOperationException("unsupported machine");
        }
    }
}
