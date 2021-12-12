import org.jctools.queues.MpscArrayQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class SkipListTest {
    public static void main(String[] args) {
        ConcurrentSkipListMap<Integer,String> test=new ConcurrentSkipListMap<>();
        test.put(1,"1");
        test.put(2,"2");
        System.out.println(test.get(3));
    }
}
