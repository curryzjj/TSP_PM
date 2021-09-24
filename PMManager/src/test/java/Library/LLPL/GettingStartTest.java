package Library.LLPL;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GettingStartTest {
    public static Logger LOG= LoggerFactory.getLogger(GettingStartTest.class);
    private GettingStarted started;
    public void setup(){
        started=new GettingStarted();
    }
    public void test1(){
        setup();
        LOG.info("Test Start:"+"Path="+started.path+"name="+started.name);
        started.createHeap();
        if(started.freeHeap(started.path+started.name)){
            LOG.info("freeHeap successfully");
        }
    }
}
