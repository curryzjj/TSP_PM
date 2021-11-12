package engine.checkpoint;

import System.FileSystem.DataIO.DataOutputView;
import System.FileSystem.DataIO.DataOutputViewStreamWrapper;
import System.util.IOUtil;
import engine.Meta.StateMetaInfoSnapshotReadersWriters;
import engine.checkpoint.CheckpointStream.CheckpointStreamFactory;
import engine.checkpoint.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.checkpoint.ShapshotResources.FullSnapshotResources;
import engine.table.datatype.serialize.Serialize;
import engine.table.keyGroup.KeyGroupRangeOffsets;
import utils.CloseableRegistry.CloseableRegistry;
import utils.StateIterator.KeyValueStateIterator;
import utils.SupplierWithException;
import utils.TransactionalProcessConstants.CheckpointType;

import java.io.IOException;
import java.io.OutputStream;

import static utils.FullSnapshotUtil.*;

public class FullSnapshotAsyncWrite implements SnapshotStrategy.SnapshotResultSupplier{

    /** Supplier for the stream into which we write the snapshot. */
    private final SupplierWithException<CheckpointStreamWithResultProvider,Exception> checkpointStreamSupplier;
    private final FullSnapshotResources snapshotResources;
    private final CheckpointType checkpointType;

    public FullSnapshotAsyncWrite(SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
                                  FullSnapshotResources snapshotResources,
                                  CheckpointType checkpointType) {
        this.checkpointStreamSupplier = checkpointStreamSupplier;
        this.snapshotResources = snapshotResources;
        this.checkpointType = checkpointType;
    }

    @Override
    public SnapshotResult get(CloseableRegistry snapshotCloseableRegistry) throws Exception {
        final KeyGroupRangeOffsets keyGroupRangeOffsets =
                new KeyGroupRangeOffsets(snapshotResources.getKeyGroupRange());
        final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider =
                checkpointStreamSupplier.get();
        snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
        writeSnapshotToOutputStream(checkpointStreamWithResultProvider, keyGroupRangeOffsets);
        if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {
         return new SnapshotResult();
        } else {
            throw new IOException("Stream is already unregistered/closed.");
        }
    }
    private void writeSnapshotToOutputStream(CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {
        //TODO:implement after deciding what to Snapshot
        final DataOutputView outputView =
                new DataOutputViewStreamWrapper(
                        checkpointStreamWithResultProvider.getCheckpointOutputStream());
        try(KeyValueStateIterator keyValueStateIterator=snapshotResources.createKVStateIterator()){
            writeKVStateData(keyValueStateIterator,checkpointStreamWithResultProvider,keyGroupRangeOffsets);
        }
        writeKVStateMetaData(outputView);
    }
    private void writeKVStateMetaData(DataOutputView outputView) throws IOException {
        outputView.writeShort(snapshotResources.getMetaInfoSnapshots().size());
        for (StateMetaInfoSnapshot metaInfoSnapshot : snapshotResources.getMetaInfoSnapshots()) {
            StateMetaInfoSnapshotReadersWriters.getWriter().writeStateMetaInfoSnapshot(metaInfoSnapshot,outputView);
        }
    }
    private void writeKVStateData(final KeyValueStateIterator mergeIterator,
                                  final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                                  KeyGroupRangeOffsets keyGroupRangeOffsets) throws IOException, InterruptedException {
        byte[] previousKey = null;
        byte[] previousValue = null;
        DataOutputView kgOutView = null;
        OutputStream kgOutStream = null;
        CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
                checkpointStreamWithResultProvider.getCheckpointOutputStream();

        try {

            // preamble: setup with first key-group as our lookahead
            if (mergeIterator.isValid()) {
                // begin first key-group by recording the offset
                keyGroupRangeOffsets.setKeyGroupOffset(
                        mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                // write the k/v-state id as metadata
                kgOutStream =checkpointOutputStream;
                kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                // TODO this could be aware of keyGroupPrefixBytes and write only one byte
                // if possible
                kgOutView.writeShort(mergeIterator.kvStateId());
                previousKey = mergeIterator.key();
                previousValue = mergeIterator.value();
                mergeIterator.next();
            }

            // main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking
            // key-group offsets.
            while (mergeIterator.isValid()) {

                assert (!hasMetaDataFollowsFlag(previousKey));

                // set signal in first key byte that meta data will follow in the stream
                // after this k/v pair
                if (mergeIterator.isNewKeyGroup() || mergeIterator.isNewKeyValueState()) {

                    // be cooperative and check for interruption from time to time in the
                    // hot loop
                    checkInterrupted();

                    setMetaDataFollowsFlagInKey(previousKey);
                }

                writeKeyValuePair(previousKey, previousValue, kgOutView);

                // write meta data if we have to
                if (mergeIterator.isNewKeyGroup()) {
                    // TODO this could be aware of keyGroupPrefixBytes and write only one
                    // byte if possible
                    kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                    // this will just close the outer stream
                    kgOutStream.close();
                    // begin new key-group
                    keyGroupRangeOffsets.setKeyGroupOffset(
                            mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                    // write the kev-state
                    // TODO this could be aware of keyGroupPrefixBytes and write only one
                    // byte if possible
                    kgOutStream =checkpointOutputStream;
                    kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                    kgOutView.writeShort(mergeIterator.kvStateId());
                } else if (mergeIterator.isNewKeyValueState()) {
                    // write the k/v-state
                    // TODO this could be aware of keyGroupPrefixBytes and write only one
                    // byte if possible
                    kgOutView.writeShort(mergeIterator.kvStateId());
                }

                // request next k/v pair
                previousKey = mergeIterator.key();
                previousValue = mergeIterator.value();
                mergeIterator.next();
            }

            // epilogue: write last key-group
            if (previousKey != null) {
                assert (!hasMetaDataFollowsFlag(previousKey));
                setMetaDataFollowsFlagInKey(previousKey);
                writeKeyValuePair(previousKey, previousValue, kgOutView);
                // TODO this could be aware of keyGroupPrefixBytes and write only one byte if
                // possible
                kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                // this will just close the outer stream
                kgOutStream.close();
                kgOutStream = null;
            }

        } finally {
            // this will just close the outer stream
            IOUtil.closeQuietly(kgOutStream);
        }
    }
    private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out)
            throws IOException {
        Serialize.writeSerializedKV(key,out);
        Serialize.writeSerializedKV(value,out);
    }

    private void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("RocksDB snapshot interrupted.");
        }
    }
}
