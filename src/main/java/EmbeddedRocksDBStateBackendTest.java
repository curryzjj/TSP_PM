import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.contrib.streaming.state.*;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.function.SupplierWithException;
import org.junit.runners.Parameterized;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksObject;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class EmbeddedRocksDBStateBackendTest {
    private OneShotLatch blocker;
    private OneShotLatch waiter;
    private BlockerCheckpointStreamFactory testStreamFactory;
    private RocksDBKeyedStateBackend<Integer> keyedStateBackend;
    private List<RocksObject> allCreatedCloseables;
    private ValueState<Integer> testState1;
    private ValueState<String> testState2;
    @Parameterized.Parameter(value = 0)
    public boolean enableIncrementalCheckpointing;

    @Parameterized.Parameter(value = 1)
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;
    private String dbPath=System.getProperty("user.home").concat("/hair-loss/app/RocksDB/");
    private RocksDB db = null;
    private ColumnFamilyHandle defaultCFHandle = null;
    private final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();
    public void prepareRocksDB() throws Exception{
        ColumnFamilyOptions columnOptions=optionsContainer.getColumnOptions();
        ArrayList<ColumnFamilyHandle> columnFamilyHandles=new ArrayList<>(1);
        db= RocksDBOperationUtils.openDB(dbPath,
                Collections.emptyList(),
                columnFamilyHandles,
                columnOptions,
                optionsContainer.getDbOptions());
        defaultCFHandle = columnFamilyHandles.remove(0);
    }
    public void setupRocksKeyedStateBackend() throws Exception{
        blocker = new OneShotLatch();
        waiter = new OneShotLatch();
        testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
        testStreamFactory.setBlockerLatch(blocker);
        testStreamFactory.setWaiterLatch(waiter);
        testStreamFactory.setAfterNumberInvocations(10);
        KeyGroupRange keyGroupRange=new KeyGroupRange(0,1);
        CloseableRegistry cancelStreamRegistry=new CloseableRegistry();
        JobID JOB_ID = new JobID();
        JobVertexID JOB_VERTEX_ID = new JobVertexID();
        int SUBTASK_INDEX = 0;
        File[] allocBaseFolders=new File[] {new File(System.getProperty("user.home").concat("/hair-loss/app/Checkpoint/"))};
        LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider=new LocalRecoveryDirectoryProviderImpl(
                allocBaseFolders, JOB_ID, JOB_VERTEX_ID, SUBTASK_INDEX);
        prepareRocksDB();
        RocksDBKeyedStateBackendBuilder<Integer> builder=new RocksDBKeyedStateBackendBuilder<>(   "no-op",
                        ClassLoader.getSystemClassLoader(),
                        RocksDBBackendTest.TEMP_FOLDER.newFolder(),
                        optionsContainer,
                stateName->optionsContainer.getColumnOptions(),
                        new KvStateRegistry().createTaskRegistry(new JobID(),new JobVertexID()),
                        IntSerializer.INSTANCE,
                        2,
                        keyGroupRange,
                        new ExecutionConfig(),
                        new LocalRecoveryConfig(true,localRecoveryDirectoryProvider),
                        EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP,
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        UncompressedStreamCompressionDecorator.INSTANCE,cancelStreamRegistry
                );
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend=new EmbeddedRocksDBStateBackend();
    }
}
