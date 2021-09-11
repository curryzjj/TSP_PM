package LibraryTest.LLPLTest;

import com.intel.pmem.llpl.Heap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Example02_SizingHeaps {
    private static long MB=1024L*1024L;
    private static long GB=1024*MB;
    private static Logger LOG= LoggerFactory.getLogger(Example02_SizingHeaps.class);
    public static void main(String[] args) {
        String path="/dev/pmem0/llpl/";
        LOG.info("Test start");
        //heap of fixed size
        Heap fixedHeap=Heap.createHeap(path+"fixed",100*MB);
        LOG.info("created fixed heap");
        //growable heap -- starts at minimum size and grows to all available pmem if file system
        String dir_path1="path"+"growable/";
        new File(dir_path1).mkdir();
        Heap growableHeap=Heap.createHeap(dir_path1);
        LOG.info("created growable heap");
        //growable heap with limit -- starts at minimum size and grows up to specified limit
        String dir_path2=path+"growable_with_limit/";
        new File(dir_path2).mkdir();
        Heap limitedHeap=Heap.createHeap(dir_path2,1*GB);
        LOG.info("created growable-with-limit heap");
    }
}
