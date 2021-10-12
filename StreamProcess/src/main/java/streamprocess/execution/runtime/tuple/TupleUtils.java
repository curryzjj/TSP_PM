package streamprocess.execution.runtime.tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
/**
 * used in the FieldsPartitionController
 */
public final class TupleUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TupleUtils.class);
    private TupleUtils() {
        // No instantiation
    }
    public static <T> int chooseTaskIndex(List<T> keys, int numTasks) {
        return Math.floorMod(listHashCode(keys), numTasks);
    }
    public static <T> int chooseTaskIndex(int hashcode, int numTasks) {
        return Math.floorMod(hashcode, numTasks);
    }
    private static <T> int listHashCode(List<T> alist) {
        if (alist == null) {
            return 1;
        } else {
            return Arrays.deepHashCode(alist.toArray());
        }
    }
}
