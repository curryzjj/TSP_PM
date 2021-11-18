package utils.StateIterator.ImplGroupIterator;

import System.util.IOUtil;
import System.util.Preconditions;
import scala.Tuple2;
import utils.CloseableRegistry.CloseableRegistry;
import utils.StateIterator.RocksDBStateIterator;
import utils.StateIterator.RocksIteratorWrapper;
import utils.StateIterator.ImplSingleStateIterator.RocksSingleStateIterator;
import utils.StateIterator.SingleStateIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RocksStatesPerKeyGroupMerageIterator implements RocksDBStateIterator {
    private final CloseableRegistry closeableRegistry;
    private final List<SingleStateIterator> heap;
    private boolean newKeyGroup;
    private boolean newKVState;
    private boolean valid;
    public SingleStateIterator currentSubIterator;
    private int iteratorFlag=0;

    public RocksStatesPerKeyGroupMerageIterator(
            final CloseableRegistry closeableRegistry,
            List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators)
            throws IOException {
        Preconditions.checkNotNull(closeableRegistry);
        Preconditions.checkNotNull(kvStateIterators);
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
            List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators)
            throws IOException{


        List<SingleStateIterator> iteratorPriorityQueue =
                new ArrayList<>();

        for (Tuple2<RocksIteratorWrapper, Integer> rocksIteratorWithKVStateId : kvStateIterators) {
            final RocksIteratorWrapper rocksIterator = rocksIteratorWithKVStateId._1;
            rocksIterator.seekToFirst();
            if (rocksIterator.isValid()) {
                RocksSingleStateIterator wrappingIterator =
                        new RocksSingleStateIterator(rocksIterator, rocksIteratorWithKVStateId._2);
                iteratorPriorityQueue.add(wrappingIterator);
                closeableRegistry.registerCloseable(wrappingIterator);
                closeableRegistry.unregisterCloseable(rocksIterator);
            } else {
                if (closeableRegistry.unregisterCloseable(rocksIterator)) {
                    IOUtil.closeQuietly(rocksIterator);
                }
            }
        }
        return iteratorPriorityQueue;
    }
    @Override
    public void next() throws IOException {
        newKVState =false;
        currentSubIterator.next();
    }
    public void switchIterator(){
        iteratorFlag++;
        if(iteratorFlag<heap.size()){
            currentSubIterator=heap.get(iteratorFlag);
            newKVState=true;
        }else{
            valid = false;
        }
    }

    @Override
    public int keyGroup() {
        return 0;
    }

    @Override
    public byte[] key() {
        return currentSubIterator.key();
    }

    @Override
    public byte[] value() {
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
        return this.currentSubIterator.isValid();
    }
    @Override
    public void close() {
        IOUtil.closeQuietly(closeableRegistry);

        if (heap != null) {
            heap.clear();
        }
    }
}
