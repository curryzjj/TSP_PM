import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.snapshot.RocksFullSnapshotStrategy;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.util.ResourceGuard;
import org.rocksdb.*;
import scala.Tuple2;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FlinkStatebackendTest {
    public static int computeRequiredBytesInKeyGroupPrefix(int totalKeyGroupsInJob) {
        return totalKeyGroupsInJob > (Byte.MAX_VALUE + 1) ? 2 : 1;
    }
    public static void main(String[] args) {
        Path savepointPath=new Path(System.getProperty("user.home").concat("/hair-loss/app/Checkpoint/"));
        long checkpointId = 0L;
        long timestamp=0L;
        int numberOfKeyGroups = 0;
        RocksFullSnapshotStrategy<String> checkpointStrategy;
        ResourceGuard rocksDBResourceGuard = new ResourceGuard();
        TypeSerializer<String> keySerializer= StringSerializer.INSTANCE;
        StateSerializerProvider<String> keySerializerProvider =StateSerializerProvider.fromNewRegisteredSerializer(keySerializer);
        KeyGroupRange keyGroupRange=new KeyGroupRange(0,1);
        int keyGroupPrefixBytes = computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
        LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation =
                new LinkedHashMap<>();
        LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new LinkedHashMap<>();
        LocalRecoveryConfig localRecoveryConfig=TestLocalRecoveryConfig.disabled();
        StreamCompressionDecorator keyGroupCompressionDecorator=UncompressedStreamCompressionDecorator.INSTANCE;
        final IOException testException = new IOException("Test exception");
        List<Tuple2<byte[], byte[]>> data = new ArrayList<>(10000);
        for (int i = 0; i < 100; ++i) {
            data.add(new Tuple2<>(("key:" + i).getBytes(), ("value:" + i).getBytes()));
        }
        long a=2097152;
        try {
            RocksDB db = RocksDB.open(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/"));
            WriteOptions options = new WriteOptions().setDisableWAL(true);
            ColumnFamilyHandle handle = db.createColumnFamily(new ColumnFamilyDescriptor("test".getBytes()));
            RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(db, options);
            // insert data
            for (Tuple2<byte[], byte[]> item : data) {
                writeBatchWrapper.put(handle, item._1, item._2);
            }
            writeBatchWrapper.flush();
            // valid result
            for (Tuple2<byte[], byte[]> item : data) {
                String s=new String(db.get(handle, item._1));
                System.out.println(s);
            }
            checkpointStrategy=new RocksFullSnapshotStrategy<String>(db,
                    rocksDBResourceGuard,
                    keySerializerProvider.currentSchemaSerializer(),
                    kvStateInformation,
                    registeredPQStates,
                    keyGroupRange,
                    keyGroupPrefixBytes,
                    localRecoveryConfig,
                    keyGroupCompressionDecorator);
            CheckpointStreamFactory.CheckpointStateOutputStream outputStream=new FailingStream(testException);
            OneShotLatch blocker;
            OneShotLatch waiter;
            blocker = new OneShotLatch();
            waiter = new OneShotLatch();
            BlockerCheckpointStreamFactory testStreamFactory;
            testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
            testStreamFactory.setBlockerLatch(blocker);
            testStreamFactory.setWaiterLatch(waiter);
            testStreamFactory.setAfterNumberInvocations(10);
            CheckpointOptions checkpointOptions=new CheckpointOptions(CheckpointType.CHECKPOINT,new CheckpointStorageLocationReference(System.getProperty("user.home").concat("/hair-loss/app/Checkpoint/").getBytes(StandardCharsets.UTF_8)));
            FullSnapshotResources fullSnapshotResources= checkpointStrategy.syncPrepareResources(checkpointId);
            //flush everything into db before taking a snapshot
            writeBatchWrapper.flush();
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot=new SnapshotStrategyRunner<>(checkpointStrategy.getDescription(),
                    checkpointStrategy,
                    new CloseableRegistry(),
                    ASYNCHRONOUS).snapshot(checkpointId,timestamp,testStreamFactory,checkpointOptions);
            Thread asyncSnapshotThread = new Thread(snapshot);
            asyncSnapshotThread.start();
            waiter.await(); // wait for snapshot to run
            waiter.reset();

            blocker.trigger(); // allow checkpointing to start writing
            waiter.await(); // wait for snapshot stream writing to run

        } catch (RocksDBException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
