package Library.LLPL;


public class GettingStartTest {
    private GettingStarted started;
    public void setup(){
        started=new GettingStarted();
    }
    public void test1(){
        setup();
        started.createHeap();
    }
}
