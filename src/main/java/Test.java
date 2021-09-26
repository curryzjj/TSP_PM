import java.util.HashMap;

public class Test {
    public static void main(String[] args){
        HashMap<String ,HashMap<String,String>> PClist = new HashMap<>();
        HashMap<String,String> C_PC=new HashMap<>();
        for(int i=0;i<5;i++){
            C_PC.put("c"+i,"pc"+i);
        }
        PClist.put("streamId",C_PC);
        System.out.println(PClist.get("streamId").values());
        System.out.println(PClist.get("streamId").keySet());
    }
}
