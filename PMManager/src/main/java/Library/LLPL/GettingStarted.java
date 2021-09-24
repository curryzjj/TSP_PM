package Library.LLPL;
import com.intel.pmem.llpl.AnyHeap;
import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import com.intel.pmem.llpl.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static System.Constants.Default_Heap_Name;
import static System.Constants.Default_Heap_Path;
public class GettingStarted {
    public String path=Default_Heap_Path;
    public String name=Default_Heap_Name;
    public static Logger LOG= LoggerFactory.getLogger(GettingStarted.class);
    public void createHeap(){
        boolean initialized=Heap.exists(path+name);
        //first run -- create heap
        Heap heap=initialized ? Heap.openHeap(path+name):Heap.createHeap(path+name);
        LOG.info("create heap");
        if(!initialized){
            //create block
            MemoryBlock block=heap.allocateMemoryBlock(256,false);
            heap.setRoot(block.handle());
            //durable write at block offset 0
            block.setLong(0,12345);
            block.flush(0,Long.BYTES);
            LOG.info("wrote and flushed value 12345");
            //transactional write at offset 0
            Transaction.create(heap,()->{
                block.addToTransaction(8,Long.BYTES);
                block.setLong(8,23456);
                LOG.info("wrote and flushed value 23456");
            });
            //allocate another block and link to it to first block
            MemoryBlock otherBlock=heap.allocateMemoryBlock(256,false);
            otherBlock.setInt(0,111);
            otherBlock.flush(0,Integer.BYTES);
            block.setLong(16,otherBlock.handle());
            block.flush(16,Long.BYTES);
        }else{
            long blockHandle=heap.getRoot();
            MemoryBlock block=heap.memoryBlockFromHandle(blockHandle);
            long value1=block.getLong(0);
            LOG.info("get value1"+value1);
            long value2=block.getLong(8);
            LOG.info("get value2"+value2);
            MemoryBlock otherBlock=heap.memoryBlockFromHandle(block.getLong(16));
            int otherValue=otherBlock.getInt(0);
            LOG.info("get otherValue"+otherValue);
            otherBlock.free(false);
            block.free(false);
            heap.setRoot(0);
        }
    }
    public boolean freeHeap(String path){
        boolean ret=false;
        Path pathToDeleted=new File(path).toPath();
        if(Files.exists(pathToDeleted)){
            try{
                Files.walk(pathToDeleted).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(!Files.exists(pathToDeleted)){
            ret=true;
        }
        return ret;
    }
}
