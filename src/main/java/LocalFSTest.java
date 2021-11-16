
import System.FileSystem.ImplFSDataInputStream.LocalDataInputStream;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Scanner;

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
        A a1=new A("name1","key1");
        A a2=new A("name1","key1");
        File file=new File(System.getProperty("user.home").concat("/hair-loss/app/a.log"));
//        LocalDataOutputStream localDataOutputStream=new LocalDataOutputStream(file);
//        ObjectOutputStream objectOutputStream=new ObjectOutputStream(localDataOutputStream);
//        objectOutputStream.writeObject(a1);
//        objectOutputStream.writeObject(a2);
//        objectOutputStream.close();
//        localDataOutputStream.close();
        LocalDataInputStream inputStream=new LocalDataInputStream(file);
        ObjectInputStream objectInputStream=new ObjectInputStream(inputStream);
        int a=objectInputStream.readInt();
        System.out.println(a);
        A a3= (A) objectInputStream.readObject();
        System.out.println(a3.key);
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
