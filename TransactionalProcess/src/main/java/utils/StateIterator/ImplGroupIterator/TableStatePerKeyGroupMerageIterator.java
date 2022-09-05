package utils.StateIterator.ImplGroupIterator;

import System.util.IOUtil;
import scala.Tuple2;
import utils.CloseableRegistry.CloseableRegistry;
import utils.StateIterator.ImplSingleStateIterator.TableSingleStateIterator;
import utils.StateIterator.InMemoryTableIteratorWrapper;
import utils.StateIterator.RocksDBStateIterator;
import utils.StateIterator.SingleStateIterator;
import utils.StateIterator.TableStateIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableStatePerKeyGroupMerageIterator implements TableStateIterator {
    private final CloseableRegistry closeableRegistry;
    private final List<SingleStateIterator> heap;
    private boolean newKVState;
    private boolean valid;
    public SingleStateIterator currentSubIterator;
    private int iteratorFlag=0;
    private long offset;

    public TableStatePerKeyGroupMerageIterator(CloseableRegistry closeableRegistry,List<Tuple2<InMemoryTableIteratorWrapper, Integer>> kvStateIterators, long offset) throws IOException {
        this.offset = offset;
        this.closeableRegistry = closeableRegistry;
        if (kvStateIterators.size() > 0) {
            this.heap = buildIteratorHeap(kvStateIterators);
            this.valid = !heap.isEmpty();
            this.currentSubIterator = heap.get(iteratorFlag);
            kvStateIterators.clear();
        } else {
            // creating a PriorityQueue of size 0 results in an exception.
            this.heap = null;
            this.valid = false;
        }
        this.newKVState = true;
    }
    private List<SingleStateIterator> buildIteratorHeap(
            List<Tuple2<InMemoryTableIteratorWrapper, Integer>> kvStateIterators)
            throws IOException{


        List<SingleStateIterator> iteratorPriorityQueue =
                new ArrayList<>();

        for (Tuple2<InMemoryTableIteratorWrapper, Integer> tableIteratorWithKVStateId : kvStateIterators) {
            final InMemoryTableIteratorWrapper inMemoryTableIterator = tableIteratorWithKVStateId._1;
            if (inMemoryTableIterator.isValid()) {
                TableSingleStateIterator wrappingIterator=new TableSingleStateIterator(inMemoryTableIterator,tableIteratorWithKVStateId._2,offset);
                iteratorPriorityQueue.add(wrappingIterator);
                closeableRegistry.registerCloseable(wrappingIterator);
                closeableRegistry.unregisterCloseable(inMemoryTableIterator);
            } else {
                if (closeableRegistry.unregisterCloseable(inMemoryTableIterator)) {
                    IOUtil.closeQuietly(inMemoryTableIterator);
                }
            }
        }
        return iteratorPriorityQueue;
    }
    @Override
    public void switchIterator() {
        iteratorFlag ++;
        if(iteratorFlag < heap.size()){
            currentSubIterator = heap.get(iteratorFlag);
            newKVState = true;
        }else{
            valid = false;
        }
    }

    @Override
    public byte[] nextkey() {
        this.newKVState=false;
        return currentSubIterator.key();
    }

    @Override
    public byte[] nextvalue() {
        return currentSubIterator.value();
    }

    @Override
    public int kvStateId() {
        return currentSubIterator.getKvStateId();
    }

    @Override
    public boolean isNewKeyValueState() {
        return newKVState;
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public boolean isIteratorValid() {
        return this.currentSubIterator.hasNext();
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(closeableRegistry);
        if (heap != null) {
            heap.clear();
        }
    }
}
