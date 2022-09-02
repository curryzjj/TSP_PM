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
    public static ArrayList<String> sortString(Set<String> keySets) {
        ArrayList<String> keys = new ArrayList<>(keySets);
        Collections.sort(keys, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if (o1.compareToIgnoreCase(o2) > 0) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
        return keys;
    }
    public static ArrayList<String> sortStringByInt(Set<String> keySets) {
        ArrayList<String> keys = new ArrayList<>(keySets);
        Collections.sort(keys, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.parseInt(o1) > Integer.parseInt(o2) ? 1 : -1;
            }
        });
        return keys;
    }
}
