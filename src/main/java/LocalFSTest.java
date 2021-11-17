
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataInputStream.LocalDataInputStream;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.UUID;

public class LocalFSTest {
    public static byte[] serializeObject(Object o) throws IOException{
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Path basepath=new Path(System.getProperty("user.home").concat("/hair-loss/app/txntp/snapshot"));
        final String fileName = UUID.randomUUID().toString();
        LocalFileSystem localFileSystem=new LocalFileSystem();
        Path path=new Path(basepath,fileName);
        localFileSystem.create(path);
    }

}
class A implements Serializable{
    String name;
    String key;
    public A(String name,String key){
        this.key=key;
        this.name=name;
    }
}
