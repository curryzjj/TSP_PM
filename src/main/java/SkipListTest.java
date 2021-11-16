import java.util.concurrent.ConcurrentSkipListSet;

public class SkipListTest {
    public static void main(String[] args) {
        ConcurrentSkipListSet<String> cc=new ConcurrentSkipListSet();
        String str1="1";
        cc.add(str1);
        String str2="2";
        cc.add(str2);
        while (true){
            String operation=cc.pollFirst();
            if(operation==null) return;
            System.out.println(operation);
        }
    }
}
