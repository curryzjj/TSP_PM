import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.*;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;


public class RocksDBBackendTest {
    private final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();
    private String dbPath=System.getProperty("user.home").concat("/hair-loss/app/RocksDB/");
    private RocksDB db = null;
    private ColumnFamilyHandle defaultCFHandle = null;
    private RocksDBKeyedStateBackend<Integer> keyedStateBackend;
    public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
    public boolean enableIncrementalCheckpointing;
    public void prepareRocksDB() throws Exception{
        ColumnFamilyOptions columnOptions=optionsContainer.getColumnOptions();
        ArrayList<ColumnFamilyHandle> columnFamilyHandles=new ArrayList<>(1);
        db= RocksDBOperationUtils.openDB(dbPath, Collections.emptyList(),columnFamilyHandles,columnOptions,optionsContainer.getDbOptions());
        defaultCFHandle = columnFamilyHandles.remove(0);
    }
//    protected ConfigurableStateBackend getStateBackend() throws IOException{
//        EmbeddedRocksDBStateBackend backend=new EmbeddedRocksDBStateBackend(enableIncrementalCheckpointing);
//        Configuration configuration=new Configuration();
//        backend.setDbStoragePath(dbPath);
//        return backend;
//    }
    public void setupRocksKeyedStateBackend() throws Exception{
        prepareRocksDB();
//        RocksDBKeyedStateBackendBuilder builder=new RocksDBKeyedStateBackendBuilder("no-op",
//                ClassLoader.getSystemClassLoader(),
//                TEMP_FOLDER.newFolder(),
//                optionsContainer,
//                optionsContainer.getColumnOptions(),
//                new KvStateRegistry().createTaskRegistry(new JobID(),new JobVertexID()),
//                IntSerializer.INSTANCE,
//                2,
//                new ExecutionConfig(),
//                false,
//                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP,
//                TtlTimeProvider.DEFAULT,
//                false,
//                new UnregisteredMetricsGroup(),
//                Collections.emptyList(),
//                UncompressedStreamCompressionDecorator.INSTANCE,
//                defaultCFHandle,
//                new CloseableRegistry());
    }
}
