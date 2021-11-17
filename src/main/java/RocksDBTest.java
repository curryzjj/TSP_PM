
import org.rocksdb.*;
import scala.Tuple2;
import utils.CloseableRegistry.CloseableRegistry;
import utils.StateIterator.RocksIteratorWrapper;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RocksDBTest {
    public static void main(String[] args) throws Exception {
        String checkpointPath = System.getProperty("user.home").concat("/hair-loss/app/Checkpoint/");
        RocksDB.loadLibrary();
        WriteOptions options = new WriteOptions().setDisableWAL(true);
        RocksDB db = RocksDB.open(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/"));
        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor("test".getBytes(StandardCharsets.UTF_8));
        ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(columnFamilyDescriptor);
        ColumnFamilyDescriptor columnFamilyDescriptor1= new ColumnFamilyDescriptor("test2".getBytes(StandardCharsets.UTF_8));
        ColumnFamilyHandle columnFamilyHandle1 = db.createColumnFamily(columnFamilyDescriptor1);
        byte[] value;
        List<Tuple2<RocksIteratorWrapper,Integer>> rocks=new ArrayList<>();
        List<byte[]> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        db.put(columnFamilyHandle1,"hello".getBytes(), "world".getBytes());
        try (final WriteOptions writeOpt = new WriteOptions()) {
            for (int i = 10; i <= 19; ++i) {
                try (final WriteBatch batch = new WriteBatch()) {
                    for (int j = 10; j <= 19; ++j) {
                        batch.put(String.format("%dx%d", i, j).getBytes(),
                                String.format("%d", i * j).getBytes());
                    }
                    db.write(writeOpt, batch);
                }
            }
            value = db.get("hello".getBytes());

        }
        final RocksIteratorWrapper iterator =new RocksIteratorWrapper(db.newIterator(columnFamilyHandle));
        final RocksIteratorWrapper iterator1 =new RocksIteratorWrapper(db.newIterator(columnFamilyHandle1));
        rocks.add(new Tuple2<>(iterator,1));
        rocks.add(new Tuple2<>(iterator,2));
        CloseableRegistry closeableRegistry=new CloseableRegistry();
        RocksStatePerKeyGroupMerageIterator rocksStatePerKeyGroupMerageIterator=new RocksStatePerKeyGroupMerageIterator(closeableRegistry,rocks,1);
        while (rocksStatePerKeyGroupMerageIterator.isValid()){
            keys.add(rocksStatePerKeyGroupMerageIterator.key());
            values.add(rocksStatePerKeyGroupMerageIterator.value());
            rocksStatePerKeyGroupMerageIterator.next();
        }
//        for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
//            keys.add(iterator.key());
//            values.add(iterator.value());
//            }
        System.out.println("2");
    }


}
