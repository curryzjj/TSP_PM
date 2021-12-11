import org.jctools.queues.MpscArrayQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

public class SkipListTest {
    public static void main(String[] args) {
        int[] a=new int[3];
        a[0]=10;
        a[1]=8;
        a[2]=11;
        Arrays.sort(a,0,2);
        System.out.println(a[0]);
    }
}
