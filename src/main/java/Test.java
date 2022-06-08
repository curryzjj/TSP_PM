import java.util.HashMap;

public class Test {

    public static void main(String[] args) {
        HashMap<Integer,B> b = new HashMap<>();
        C c = new C(0,b);
        c.interrupt();
        System.out.println(c.b.a);
    }
}

class C extends Thread{
    public B b;
    public C(int id,HashMap<Integer, B> b){
        this.b = new B();
        b.put(id,this.b);
    }
}
 class B{
    public int a;
    public B(){
        this.a =4;
    }
}
