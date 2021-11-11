package engine.checkpoint;

import engine.checkpoint.CheckpointStream.CheckpointStreamWithResultProvider;
import engine.checkpoint.ShapshotResources.FullSnapshotResources;
import utils.CloseableRegistry;
import utils.SupplierWithException;
import utils.TransactionalProcessConstants.CheckpointType;

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
        return null;
    }
    private void writeSnapshotToOutputStream(){
        //TODO:implement after deciding what to Snapshot
    }
    private void writeKVStateMetaData(){

    }
    private void writeKVStateData(){

    }
}
