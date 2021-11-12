package engine.checkpoint.ShapshotResources.ImplShapshotResources;

import System.util.IOUtil;
import engine.checkpoint.ShapshotResources.FullSnapshotResources;
import engine.checkpoint.StateMetaInfoSnapshot;
import engine.storage.ImplStorageManager.RocksDBManager;
import engine.table.keyGroup.KeyGroupRange;
import org.rocksdb.*;
import scala.Tuple2;
import utils.CloseableRegistry.CloseableRegistry;
import utils.ResourceGuard;
import utils.StateIterator.KeyValueStateIterator;
import utils.StateIterator.RocksIteratorWrapper;
import utils.StateIterator.RocksStatesPerKeyGroupMerageIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class RocksDBFullSnapshotResources implements FullSnapshotResources {
    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
    private final ResourceGuard.Lease lease;
    private final RocksDB db;
    private final Snapshot snapshot;
    private final List<RocksDBManager.RocksDBKvStateInfo> Meta;
    private final KeyGroupRange keyGroupRange;

    public RocksDBFullSnapshotResources(
            ResourceGuard.Lease lease,
            RocksDB db,
            Snapshot snapshot,
            List<RocksDBManager.RocksDBKvStateInfo> metaDataCopy,
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots, KeyGroupRange keyGroupRange) {
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        this.lease = lease;
        this.db = db;
        this.snapshot = snapshot;
        this.Meta = metaDataCopy;
        this.keyGroupRange = keyGroupRange;
    }
    public static RocksDBFullSnapshotResources create(final LinkedHashMap<String, RocksDBManager.RocksDBKvStateInfo> kvStateInformation,
                                                      final RocksDB db,
                                                      final ResourceGuard rocksDBResourceGuard,
                                                      final KeyGroupRange keyGroupRange) throws IOException{
        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots=new ArrayList<>(kvStateInformation.size());
        final List<RocksDBManager.RocksDBKvStateInfo> metaDataCopy=new ArrayList<>(kvStateInformation.size());
        for(RocksDBManager.RocksDBKvStateInfo stateInfo:kvStateInformation.values()){
            //snapshot mate info for each state
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }
        final ResourceGuard.Lease lease = rocksDBResourceGuard.acquireResource();
        final Snapshot snapshot = db.getSnapshot();
        return new RocksDBFullSnapshotResources(lease,db,snapshot,metaDataCopy,stateMetaInfoSnapshots, keyGroupRange);
    }

    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
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
        CloseableRegistry closeableRegistry=new CloseableRegistry();
        try{
            ReadOptions readOptions=new ReadOptions();
            closeableRegistry.registerCloseable(readOptions::close);
            readOptions.setSnapshot(snapshot);
            List<Tuple2<RocksIteratorWrapper,Integer>> kvStateIterators=createKVStateIterators(closeableRegistry,readOptions);
            return new RocksStatesPerKeyGroupMerageIterator(closeableRegistry,kvStateIterators,1);
        } catch (IOException e) {
            // If anything goes wrong, clean up our stuff. If things went smoothly the
            // merging iterator is now responsible for closing the resources
            IOUtil.closeQuietly(closeableRegistry);
            throw new IOException("Error creating merge iterator");
        }
    }
    private List<Tuple2<RocksIteratorWrapper,Integer>> createKVStateIterators(CloseableRegistry closeableRegistry,ReadOptions readOptions) throws IOException {
        final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
                new ArrayList<>(Meta.size());
        int kvStateId = 0;
        for (RocksDBManager.RocksDBKvStateInfo stateInfo:Meta){
            RocksIteratorWrapper rocksIteratorWrapper=createRocksIteratorWrapper(db,stateInfo.columnFamilyHandle,readOptions);
            kvStateIterators.add(new Tuple2<>(rocksIteratorWrapper,kvStateId));
            closeableRegistry.registerCloseable(rocksIteratorWrapper);
            ++kvStateId;
        }
        return kvStateIterators;
    }
    private static RocksIteratorWrapper createRocksIteratorWrapper(RocksDB db,
                                                                   ColumnFamilyHandle columnFamilyHandle,
                                                                   ReadOptions readOptions){
        RocksIterator rocksIterator=db.newIterator(columnFamilyHandle,readOptions);
        return new RocksIteratorWrapper(rocksIterator);
    }
}
