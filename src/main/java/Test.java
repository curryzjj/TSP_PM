import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinitySupport;

import java.util.HashMap;

public class Test {

    public static void main(String[] args) {
        B b=new B();
        System.out.println(b.a);
    }
}

class C{
    public int a;
    public C(){
        System.out.println("A");
        this.a=0;
    }
    public C(String a){
        this.a=4;
        System.out.println("A1");
    }
    public void setA(int a) {
        this.a = a;
    }
}
 class B extends C{
    public int a;
    public B(){
        super("a");
        //this.a=4;
    }
}
