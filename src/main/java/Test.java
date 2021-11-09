import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinitySupport;

public class Test {
    static class A{
        public int a;
        public A(){
            this.a=0;
        }
        public A(String a){
            System.out.println("A");
        }
        public void setA(int a) {
            this.a = a;
        }
    }
    static class B extends A{
        public B(){
            //super("a");
        }
    }
    public static void main(String[] args) {

    }
}
