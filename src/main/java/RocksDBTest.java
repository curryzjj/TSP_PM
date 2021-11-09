import org.apache.flink.core.fs.Path;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;

public class RocksDBTest {
    public static void main(String[] args) throws Exception{
        String checkpointPath=System.getProperty("user.home").concat("/hair-loss/app/Checkpoint/");
        RocksDB.loadLibrary();
        RocksDB db=RocksDB.open(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/"));
        Options options=new Options();
        byte[] key = new byte[4];
        byte[] value;
        for (int i=0;i<10;i++){
            int k=i;
            String v=""+i;
            value=v.getBytes(StandardCharsets.UTF_8);
            key[0] = (byte)(i >> 24 & 0xFF);
            key[1] = (byte)(i >> 16 & 0xFF);
            key[2] = (byte)(i >> 8 & 0xFF);
            key[3] = (byte)(i & 0xFF);
            db.put(key,value);
        }
        int i=0;
        key[0] = (byte)(i >> 24 & 0xFF);
        key[1] = (byte)(i >> 16 & 0xFF);
        key[2] = (byte)(i >> 8 & 0xFF);
        key[3] = (byte)(i & 0xFF);
        db.put(key,"dd".getBytes(StandardCharsets.UTF_8));
        for (i=0;i<10;i++){
            int k=i;
            key[0] = (byte)(i >> 24 & 0xFF);
            key[1] = (byte)(i >> 16 & 0xFF);
            key[2] = (byte)(i >> 8 & 0xFF);
            key[3] = (byte)(i & 0xFF);
            value=db.get(key);
            String s=new String(value,"UTF-8");
            System.out.println(s);
        }
        i=0;
        key[0] = (byte)(i >> 24 & 0xFF);
        key[1] = (byte)(i >> 16 & 0xFF);
        key[2] = (byte)(i >> 8 & 0xFF);
        key[3] = (byte)(i & 0xFF);
        db.delete(key);
        for (i=1;i<10;i++){
            int k=i;
            key[0] = (byte)(i >> 24 & 0xFF);
            key[1] = (byte)(i >> 16 & 0xFF);
            key[2] = (byte)(i >> 8 & 0xFF);
            key[3] = (byte)(i & 0xFF);
            value=db.get(key);
            String s=new String(value,"UTF-8");
            System.out.println(s);
        }
        Checkpoint checkpoint=Checkpoint.create(db);
        checkpoint.createCheckpoint(checkpointPath+"ch-0");
        RocksDB db1=RocksDB.open(checkpointPath+"ch-0");
        for (i=1;i<10;i++){
            int k=i;
            key[0] = (byte)(i >> 24 & 0xFF);
            key[1] = (byte)(i >> 16 & 0xFF);
            key[2] = (byte)(i >> 8 & 0xFF);
            key[3] = (byte)(i & 0xFF);
            value=db1.get(key);
            String s=new String(value,"UTF-8");
            System.out.println(s);
        }
    }
}
