import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinitySupport;

public class Test {
    static class A{
        public A(){
            System.out.println("a");
        }
        public A(String a){
            System.out.println("A");
        }
    }
    static class B extends A{
        public B(){
            //super("a");
        }
    }
    public static void main(String[] args) {
        int cpuId = AffinitySupport.getThreadId();
        System.out.println(cpuId);
        try (AffinityLock al = AffinityLock.acquireCore()) {
            // do some work while locked to a CPU.
            System.out.println("Main locked");
        }

    }
}
