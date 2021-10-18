import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;

import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static xerial.jnuma.Numa.setLocalAlloc;

public class AffinityLockTest {
    public static void main(String arg[]){
        long[] cpu=new long[2];
                // do some work while locked to a CPU.
                setLocalAlloc();
                AffinityLock al=AffinityLock.acquireLock((int) cpu[0]);
                System.out.println(al.cpuId());
                System.out.println(Affinity.getAffinity());
    }
}
