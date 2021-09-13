package LibraryTest.LLPLTest;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import com.intel.pmem.llpl.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Example01_GettingStarted {
    public static Logger LOG= LoggerFactory.getLogger(Example01_GettingStarted.class);
    public static void main(String[] args){
        LOG.info("Test Start");
        String path="";
        boolean initialized=Heap.exists(path);
        //first run -- create heap
       Heap heap=initialized ? Heap.openHeap(path):Heap.createHeap(path);
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
            long value2=block.getLong(8);
            MemoryBlock otherBlock=heap.memoryBlockFromHandle(block.getLong(16));
            int otherValue=otherBlock.getInt(0);
            otherBlock.free(false);
            block.free(false);
            heap.setRoot(0);
        }

    }
}
