
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RocksDBTest {
    public static void main(String[] args) throws Exception{
        String checkpointPath=System.getProperty("user.home").concat("/hair-loss/app/Checkpoint/");
        RocksDB.loadLibrary();
        WriteOptions options = new WriteOptions().setDisableWAL(true);
        RocksDB db=RocksDB.open(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/"));
        ColumnFamilyDescriptor columnFamilyDescriptor=new ColumnFamilyDescriptor("test".getBytes(StandardCharsets.UTF_8));
        ColumnFamilyHandle columnFamilyHandle=db.createColumnFamily(columnFamilyDescriptor);
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
            db.put(columnFamilyHandle,key,value);
        }
        int i=0;
        key[0] = (byte)(i >> 24 & 0xFF);
        key[1] = (byte)(i >> 16 & 0xFF);
        key[2] = (byte)(i >> 8 & 0xFF);
        key[3] = (byte)(i & 0xFF);
        db.put(columnFamilyHandle,key,"dd".getBytes(StandardCharsets.UTF_8));
        for (i=0;i<10;i++){
            int k=i;
            key[0] = (byte)(i >> 24 & 0xFF);
            key[1] = (byte)(i >> 16 & 0xFF);
            key[2] = (byte)(i >> 8 & 0xFF);
            key[3] = (byte)(i & 0xFF);
            value=db.get(columnFamilyHandle,key);
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
            value=db.get(columnFamilyHandle,key);
            String s=new String(value,"UTF-8");
            System.out.println(s);
        }

        Checkpoint checkpoint=Checkpoint.create(db);
        checkpoint.createCheckpoint(checkpointPath+"ch-0");//hard-link
        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
        columnFamilyDescriptorList.add(columnFamilyDescriptor);
        columnFamilyDescriptorList.add(db.getDefaultColumnFamily().getDescriptor());
        List<ColumnFamilyHandle> columnFamilyHandleList=new ArrayList<>();
        columnFamilyHandleList.add(columnFamilyHandle);
        columnFamilyHandleList.add(db.getDefaultColumnFamily());
        RocksDB db1=RocksDB.open(checkpointPath+"ch-0",columnFamilyDescriptorList,columnFamilyHandleList);
        for (i=1;i<10;i++){
            int k=i;
            key[0] = (byte)(i >> 24 & 0xFF);
            key[1] = (byte)(i >> 16 & 0xFF);
            key[2] = (byte)(i >> 8 & 0xFF);
            key[3] = (byte)(i & 0xFF);
            value=db1.get(columnFamilyHandle,key);
            String s=new String(value,"UTF-8");
            System.out.println(s);
        }
    }
}
