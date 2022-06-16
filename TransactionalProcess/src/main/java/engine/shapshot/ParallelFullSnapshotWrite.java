package engine.shapshot;

import System.FileSystem.DataIO.DataOutputView;
import System.FileSystem.DataIO.DataOutputViewStreamWrapper;
import System.FileSystem.Path;
import engine.Meta.StateMetaInfoSnapshotReadersWriters;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.shapshot.ShapshotResources.FullSnapshotResources;
import engine.table.datatype.serialize.Serialize;
import engine.table.keyGroup.KeyGroupRangeOffsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import utils.CloseableRegistry.CloseableRegistry;
import utils.StateIterator.ImplGroupIterator.TableStatePerKeyGroupMerageIterator;
import utils.StateIterator.RocksDBStateIterator;
import utils.TransactionalProcessConstants.CheckpointType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static UserApplications.CONTROL.PARTITION_NUM;
import static engine.Database.snapshotExecutor;
import static utils.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;

public class ParallelFullSnapshotWrite implements SnapshotStrategy.SnapshotResultSupplier {
    private final Logger LOG= LoggerFactory.getLogger(ParallelFullSnapshotWrite.class);
    private final List<CheckpointStreamWithResultProvider> providers;
    private final List<FullSnapshotResources> snapshotResources;
    private final long timestamp;
    private final long checkpointId;
    private final CheckpointType checkpointType;
    private final CheckpointOptions options;

    public ParallelFullSnapshotWrite(List<CheckpointStreamWithResultProvider> providers,List<FullSnapshotResources> snapshotResources, long timestamp, long checkpointId, CheckpointType checkpointType,CheckpointOptions options) {
        this.providers = providers;
        this.timestamp = timestamp;
        this.checkpointId = checkpointId;
        this.checkpointType = checkpointType;
        this.snapshotResources = snapshotResources;
        this.options = options;
    }

    @Override
    public SnapshotResult get(CloseableRegistry snapshotCloseableRegistry) throws Exception {
        Collection<SnapshotTask> callables = new ArrayList<>();
        initTasks(callables,snapshotCloseableRegistry);
        List<Future<Tuple2<Path,KeyGroupRangeOffsets>>> snapshotPaths = snapshotExecutor.invokeAll(callables);
        HashMap<Integer, Tuple2<Path,KeyGroupRangeOffsets>> results = new HashMap<>();
        for(int i = 0; i < PARTITION_NUM; i++){
            try {
                Tuple2<Path,KeyGroupRangeOffsets> tuple = snapshotPaths.get(i).get();
                results.put(i, tuple);
            } catch (ExecutionException e) {
                System.out.println(e.getMessage());
            } catch (CancellationException e) {
                System.out.println("Cancel");
            }
        }
        return new SnapshotResult(results, timestamp, checkpointId);
    }
    private void initTasks(Collection<SnapshotTask> callables, CloseableRegistry closeableRegistry) {
        for(int i = 0; i < options.partitionNum; i ++){
            callables.add(new SnapshotTask( i, snapshotResources.get(i), closeableRegistry, providers.get(i)));
        }
    }
    private class SnapshotTask implements Callable<Tuple2<Path,KeyGroupRangeOffsets>>{
        private CheckpointStreamWithResultProvider provider;
        private int taskId;
        private FullSnapshotResources snapshotResources;
        private CloseableRegistry snapshotCloseableRegistry;
        private SnapshotTask(int taskId,
                             FullSnapshotResources snapshotResources,
                             CloseableRegistry snapshotCloseableRegistry,
                             CheckpointStreamWithResultProvider provider) {
            this.taskId = taskId;
            this.snapshotResources = snapshotResources;
            this.snapshotCloseableRegistry=snapshotCloseableRegistry;
            this.provider = provider;
        }

        @Override
        public Tuple2<Path,KeyGroupRangeOffsets> call() throws Exception {
            final KeyGroupRangeOffsets keyGroupRangeOffsets =
                    new KeyGroupRangeOffsets(snapshotResources.getKeyGroupRange());
            snapshotCloseableRegistry.registerCloseable(provider);
            writeSnapshotToOutputStream(provider, snapshotResources, keyGroupRangeOffsets, this.taskId);
            if (snapshotCloseableRegistry.unregisterCloseable(provider)) {
                 Path path = provider.closeAndFinalizeCheckpointStreamResult();
                 return new Tuple2<Path, KeyGroupRangeOffsets>(path,keyGroupRangeOffsets);
            } else {
                throw new IOException("Stream is already unregistered/closed.");
            }
        }
    }
    private void writeSnapshotToOutputStream(CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,FullSnapshotResources snapshotResources,KeyGroupRangeOffsets keyGroupRangeOffsets,int taskId) throws IOException, InterruptedException {
        final DataOutputView outputView =
                new DataOutputViewStreamWrapper(
                        checkpointStreamWithResultProvider.getCheckpointOutputStream());
        writeKVStateMetaData(outputView,snapshotResources);
        switch(checkpointType){
            case RocksDBFullSnapshot:
                try(RocksDBStateIterator keyValueStateIterator= (RocksDBStateIterator) snapshotResources.createKVStateIterator()){
                    writeRocksDBKVStateData(keyValueStateIterator,checkpointStreamWithResultProvider,keyGroupRangeOffsets);
                }
                break;
            case InMemoryFullSnapshot:
                try(TableStatePerKeyGroupMerageIterator keyValueStateIterator= (TableStatePerKeyGroupMerageIterator) snapshotResources.createKVStateIterator()){
                    writeTableKVStateData(keyValueStateIterator,checkpointStreamWithResultProvider,keyGroupRangeOffsets,taskId);
                }
                break;
        }

    }
    private void writeTableKVStateData(TableStatePerKeyGroupMerageIterator mergeIterator,
                                       CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                                       KeyGroupRangeOffsets keyGroupRangeOffsets,int taskId) throws IOException {
        DataOutputView kgOutView = null;
        OutputStream kgOutStream = null;
        CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
                checkpointStreamWithResultProvider.getCheckpointOutputStream();
        kgOutStream = checkpointOutputStream;
        kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
        try{
            while(mergeIterator.isValid()){
                if(mergeIterator.isNewKeyValueState()){
                    kgOutStream = checkpointOutputStream;
                    kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                    kgOutView.writeInt(mergeIterator.kvStateId());
                    keyGroupRangeOffsets.setKeyGroupOffset(
                            mergeIterator.kvStateId(), checkpointOutputStream.getPos());
                }
                writeKeyValuePair(mergeIterator.nextkey(), mergeIterator.nextvalue(), kgOutView);
                if(!mergeIterator.isIteratorValid()){
                    kgOutView.writeInt(END_OF_KEY_GROUP_MARK);
                    mergeIterator.switchIterator();
                }
            }
        }finally {
            kgOutStream.flush();
        }
    }

    private void writeKVStateMetaData(DataOutputView outputView,FullSnapshotResources snapshotResources) throws IOException {
        outputView.writeInt(snapshotResources.getMetaInfoSnapshots().size());
        for (StateMetaInfoSnapshot metaInfoSnapshot : snapshotResources.getMetaInfoSnapshots()) {
            StateMetaInfoSnapshotReadersWriters.getWriter().writeStateMetaInfoSnapshot(metaInfoSnapshot, outputView);
        }
    }
    private void writeRocksDBKVStateData(final RocksDBStateIterator mergeIterator,
                                         final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                                         KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {
        DataOutputView kgOutView = null;
        OutputStream kgOutStream = null;
        CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
                checkpointStreamWithResultProvider.getCheckpointOutputStream();
        kgOutStream = checkpointOutputStream;
        kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
        try{
            while(mergeIterator.isValid()){
                if(mergeIterator.isNewKeyValueState()){
                    kgOutStream =checkpointOutputStream;
                    kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                    kgOutView.writeShort(mergeIterator.kvStateId());
                    keyGroupRangeOffsets.setKeyGroupOffset(
                            mergeIterator.kvStateId(), checkpointOutputStream.getPos());
                }
                writeKeyValuePair(mergeIterator.key(),mergeIterator.value(),kgOutView);
                mergeIterator.next();
                if(!mergeIterator.isIteratorValid()){
                    kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                    mergeIterator.switchIterator();
                }
            }
        }finally {
            // this will just close the outer stream
            //IOUtil.closeQuietly(kgOutStream);
        }
    }
    private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out)
            throws IOException {
        //Serialize.writeSerializedKV(key,out);
        Serialize.writeSerializedKV(value,out);
    }

    private void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("RocksDB snapshot interrupted.");
        }
    }
}
