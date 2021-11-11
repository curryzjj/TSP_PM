package engine.checkpoint.ShapshotResources.ImplShapshotResources;

import engine.checkpoint.ImplSnapshotStrategy.RocksFullSnapshotStrategy;
import engine.checkpoint.ShapshotResources.FullSnapshotResources;
import engine.checkpoint.StateMetaInfoSnapshot;
import engine.storage.ImplStorageManager.RocksDBManager;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;
import utils.ResourceGuard;
import utils.StateIterator.KeyValueStateIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class RocksDBFullSnapshotResources implements FullSnapshotResources {
    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
    private final ResourceGuard.Lease lease;
    private final RocksDB db;
    private final Snapshot snapshot;

    public RocksDBFullSnapshotResources(
                                        ResourceGuard.Lease lease,
                                        RocksDB db,
                                        Snapshot snapshot,
                                        List<RocksDBManager.RocksDBKvStateInfo> metaDataCopy,
                                        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        this.lease = lease;
        this.db = db;
        this.snapshot = snapshot;
    }
    public static RocksDBFullSnapshotResources create(final LinkedHashMap<String, RocksDBManager.RocksDBKvStateInfo> kvStateInformation,
                                                      final RocksDB db,
                                                      final ResourceGuard rocksDBResourceGuard) throws IOException{
        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots=new ArrayList<>(kvStateInformation.size());
        final List<RocksDBManager.RocksDBKvStateInfo> metaDataCopy=new ArrayList<>(kvStateInformation.size());
        for(RocksDBManager.RocksDBKvStateInfo stateInfo:kvStateInformation.values()){
            //snapshot mate info for each state
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }
        final ResourceGuard.Lease lease = rocksDBResourceGuard.acquireResource();
        final Snapshot snapshot = db.getSnapshot();
        return new RocksDBFullSnapshotResources(lease,db,snapshot,metaDataCopy,stateMetaInfoSnapshots);
    }

    @Override
    public void release() {

    }

    @Override
    public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
        return stateMetaInfoSnapshots;
    }

    @Override
    public KeyValueStateIterator createKVStateIterator() throws IOException {
        return null;
    }
}
