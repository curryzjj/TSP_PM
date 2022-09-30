package engine.shapshot;

import System.FileSystem.DataIO.DataOutputView;
import System.FileSystem.DataIO.DataOutputViewStreamWrapper;
import engine.Meta.StateMetaInfoSnapshotReadersWriters;
import engine.shapshot.CheckpointStream.CheckpointStreamFactory;
import engine.shapshot.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.shapshot.ShapshotResources.FullSnapshotResources;
import engine.table.datatype.serialize.Serialize;
import engine.table.keyGroup.KeyGroupRangeOffsets;
import utils.CloseableRegistry.CloseableRegistry;
import utils.StateIterator.ImplGroupIterator.TableStatePerKeyGroupMerageIterator;
import utils.StateIterator.RocksDBStateIterator;
import utils.TransactionalProcessConstants.CheckpointType;

import java.io.IOException;
import java.io.OutputStream;

import static utils.FullSnapshotUtil.*;

public class FullSnapshotAsyncWrite implements SnapshotStrategy.SnapshotResultSupplier{
    /** Supplier for the stream into which we write the snapshot. */
    private final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider;
    private final FullSnapshotResources snapshotResources;
    private final CheckpointType checkpointType;
    private final long timestamp;
    private final long checkpointId;

    public FullSnapshotAsyncWrite(CheckpointStreamWithResultProvider provider,
                                  FullSnapshotResources snapshotResources,
                                  CheckpointType checkpointType,
                                  long timestamp,
                                  long checkpointId) {
        this.checkpointStreamWithResultProvider = provider;
        this.snapshotResources = snapshotResources;
        this.checkpointType = checkpointType;
        this.timestamp=timestamp;
        this.checkpointId=checkpointId;
    }

    @Override
    public SnapshotResult get(CloseableRegistry snapshotCloseableRegistry) throws Exception {
        final KeyGroupRangeOffsets keyGroupRangeOffsets =
                new KeyGroupRangeOffsets(snapshotResources.getKeyGroupRange());
        snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
        writeSnapshotToOutputStream(checkpointStreamWithResultProvider, keyGroupRangeOffsets);
        if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {
         return CheckpointStreamWithResultProvider.createSnapshotResult(checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult(),
                 keyGroupRangeOffsets,timestamp,checkpointId);
        } else {
            throw new IOException("Stream is already unregistered/closed.");
        }
    }
    private void writeSnapshotToOutputStream(CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {
        final DataOutputView outputView =
                new DataOutputViewStreamWrapper(
                        checkpointStreamWithResultProvider.getCheckpointOutputStream());
        writeKVStateMetaData(outputView);
        switch(checkpointType){
            case RocksDBFullSnapshot:
                try(RocksDBStateIterator keyValueStateIterator= (RocksDBStateIterator) snapshotResources.createKVStateIterator()){
                    writeRocksDBKVStateData(keyValueStateIterator,checkpointStreamWithResultProvider,keyGroupRangeOffsets);
                }
                break;
            case InMemoryFullSnapshot:
                try(TableStatePerKeyGroupMerageIterator keyValueStateIterator= (TableStatePerKeyGroupMerageIterator) snapshotResources.createKVStateIterator()){
                    writeTableKVStateData(keyValueStateIterator,checkpointStreamWithResultProvider,keyGroupRangeOffsets);
                }
                break;
        }

    }

    private void writeTableKVStateData(TableStatePerKeyGroupMerageIterator mergeIterator,
                                       CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                                       KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException {
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

    private void writeKVStateMetaData(DataOutputView outputView) throws IOException {
        outputView.writeInt(snapshotResources.getMetaInfoSnapshots().size());
        for (StateMetaInfoSnapshot metaInfoSnapshot : snapshotResources.getMetaInfoSnapshots()) {
            StateMetaInfoSnapshotReadersWriters.getWriter().writeStateMetaInfoSnapshot(metaInfoSnapshot,outputView);
        }
    }
    private void writeRocksDBKVStateData(final RocksDBStateIterator mergeIterator,
                                         final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                                         KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {
        DataOutputView kgOutView = null;
        OutputStream kgOutStream = null;
        CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
                checkpointStreamWithResultProvider.getCheckpointOutputStream();
        kgOutStream =checkpointOutputStream;
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
