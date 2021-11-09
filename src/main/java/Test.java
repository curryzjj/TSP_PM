import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinitySupport;

import java.util.HashMap;

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
        HashMap<String, String> hash_index_ =new HashMap<>();
        hash_index_.put("1","2");
        if(hash_index_.get("2")==null){
            System.out.println("e");
        }
    }
}
