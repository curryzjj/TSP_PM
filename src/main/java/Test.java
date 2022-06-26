import java.util.*;

public class Test {

    public static void main(String[] args) {
        HashMap<Long, Long> test = new HashMap<>();
        test.put(1600000L,1600000L);
        test.put(3200000L,3200000L);
        test.put(4800000L,4800000L);
        ArrayList<Long> keys = new ArrayList<>(test.keySet());
        keys.sort((o1, o2) -> o1 > o2 ? 1 : -1);
        Queue<Long> testQueue = new ArrayDeque<>();
        testQueue.addAll(keys);
        System.out.println(testQueue);
    }
    public static int isDigitStr(String str) {
        int num = -1;
        char[] chars = str.toCharArray();
        String numStr = null;
        StringBuffer s =new StringBuffer();
        for (int i = 0; i < chars.length; i ++) {
            if (Character.isDigit(str.charAt(i))) {
                numStr = String.valueOf(str.charAt(i));
                s.append(numStr);
                num = Integer.parseInt(s.toString());
            }
        }
        return num;
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
