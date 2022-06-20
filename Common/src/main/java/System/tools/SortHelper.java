package System.tools;

import java.util.*;

public class SortHelper {
    public static ArrayList<Long> sortKey(Set<Long> keySets) {
        ArrayList<Long> keys = new ArrayList<>(keySets);
        keys.sort((o1, o2) -> o1 > o2 ? 1 : 0);
        return keys;
    }
}
