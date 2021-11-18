package engine.shapshot.ShapshotResources.ImplShapshotResources;

import System.util.IOUtil;
import engine.shapshot.ShapshotResources.FullSnapshotResources;
import engine.shapshot.StateMetaInfoSnapshot;
import engine.storage.ImplStorageManager.StorageManager;
import engine.table.BaseTable;
import engine.table.keyGroup.KeyGroupRange;
import engine.table.tableRecords.TableRecord;
import scala.Tuple2;
import utils.CloseableRegistry.CloseableRegistry;
import utils.ResourceGuard;
import utils.StateIterator.ImplGroupIterator.TableStatePerKeyGroupMerageIterator;
import utils.StateIterator.InMemoryTableIteratorWrapper;
import utils.StateIterator.RocksDBStateIterator;
import utils.StateIterator.kvStateIterator;

import java.io.IOException;
import java.util.*;

public class InMemoryFullSnapshotResources implements FullSnapshotResources {
    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
    private final ResourceGuard.Lease lease;
    private final Map<String, BaseTable> tables;
    private final List<StorageManager.InMemoryKvStateInfo> Meta;
    private final KeyGroupRange keyGroupRange;

    public InMemoryFullSnapshotResources(ResourceGuard.Lease lease,
                                         Map<String, BaseTable> tables,
                                         List<StorageManager.InMemoryKvStateInfo> meta,
                                         List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                                         KeyGroupRange keyGroupRange) {
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        this.lease = lease;
        this.tables = tables;
        Meta = meta;
        this.keyGroupRange = keyGroupRange;
    }
    public static InMemoryFullSnapshotResources create(final LinkedHashMap<String, StorageManager.InMemoryKvStateInfo> kvStateInformation,
                                                       final Map<String, BaseTable> tables,
                                                       final ResourceGuard ResourceGuard,
                                                       final KeyGroupRange keyGroupRange) throws IOException {
        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots=new ArrayList<>(kvStateInformation.size());
        final List<StorageManager.InMemoryKvStateInfo> metaDataCopy=new ArrayList<>(kvStateInformation.size());
        for(StorageManager.InMemoryKvStateInfo stateInfo:kvStateInformation.values()){
            //snapshot mate info for each state
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }
        final ResourceGuard.Lease lease = ResourceGuard.acquireResource();
        return new InMemoryFullSnapshotResources(lease,tables,metaDataCopy,stateMetaInfoSnapshots,keyGroupRange);
    }
    @Override
    public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
        return stateMetaInfoSnapshots;
    }

    @Override
    public kvStateIterator createKVStateIterator() throws IOException {
        CloseableRegistry closeableRegistry=new CloseableRegistry();
        try{
            List<Tuple2<InMemoryTableIteratorWrapper,Integer>> kvStateIterators=createKVStateIterators(closeableRegistry);
            return new TableStatePerKeyGroupMerageIterator(closeableRegistry,kvStateIterators);
        } catch (IOException e) {
            // If anything goes wrong, clean up our stuff. If things went smoothly the
            // merging iterator is now responsible for closing the resources
            IOUtil.closeQuietly(closeableRegistry);
            throw new IOException("Error creating merge iterator");
        }
    }
    private List<Tuple2<InMemoryTableIteratorWrapper,Integer>> createKVStateIterators(CloseableRegistry closeableRegistry) throws IOException {
        final List<Tuple2<InMemoryTableIteratorWrapper, Integer>> kvStateIterators =
                new ArrayList<>(Meta.size());
        int kvStateId = 0;
        for (StorageManager.InMemoryKvStateInfo stateInfo:Meta){
            InMemoryTableIteratorWrapper IteratorWrapper=createRocksIteratorWrapper(tables,stateInfo.metaInfo.getName());
            kvStateIterators.add(new Tuple2<>(IteratorWrapper,kvStateId));
            closeableRegistry.registerCloseable(IteratorWrapper);
            ++kvStateId;
        }
        return kvStateIterators;
    }
    private static InMemoryTableIteratorWrapper createRocksIteratorWrapper(Map<String,BaseTable> tables,
                                                                   String tablename){
        Iterator<TableRecord> iterator=tables.get(tablename).iterator();
        Iterator<String> keyIterator=tables.get(tablename).keyIterator();
        return new InMemoryTableIteratorWrapper(iterator,keyIterator);
    }
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public void release() {

    }
}
