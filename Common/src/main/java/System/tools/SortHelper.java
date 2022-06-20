package System.tools;

import java.util.*;

public class SortHelper {
    public static ArrayList<Long> sortKey(Set<Long> keySets) {
        ArrayList<Long> keys = new ArrayList<>(keySets);
        Collections.sort(keys, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1 > o2 ? 1 : -1;
            }
        });
        return keys;
    }
}
