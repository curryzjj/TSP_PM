import java.util.HashMap;

public class Test {

    public static void main(String[] args) {
        String a = "segment_cnt_16";
        System.out.println(isDigitStr(a));
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
