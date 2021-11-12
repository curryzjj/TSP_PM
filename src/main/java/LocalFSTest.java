import System.FileSystem.DataIO.DataOutputView;
import System.FileSystem.DataIO.DataOutputViewStreamWrapper;
import System.FileSystem.FSDataOutputStream;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;

import java.io.*;

public class LocalFSTest {
    public static void main(String[] args) throws IOException {
        String parent=System.getProperty("user.home").concat("/hair-loss/app/RocksDB/");
        String child="/txt.txt";
        Path testPath=new Path(parent,child);
        String str="this is a new file";
        int a=890;
        File file=new File(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/")+"txt1.txt");
        FileOutputStream out=new FileOutputStream(file);
        DataOutputStream dataOutputStream=new DataOutputStream(out);
        dataOutputStream.writeInt(a);
        dataOutputStream.flush();

        LocalFileSystem localFileSystem=new LocalFileSystem();
        FSDataOutputStream outputStream=localFileSystem.create(testPath);
        DataOutputView dataOutputView=new DataOutputViewStreamWrapper(outputStream);
        dataOutputView.writeInt(a);
        dataOutputView.writeUTF("dslkdlsj");

        InputStream is = new FileInputStream(System.getProperty("user.home").concat("/hair-loss/app/RocksDB/")+"txt1.txt");
        DataInputStream dis = new DataInputStream(is);
        while (dis.available() > 0) {
            int k = dis.readInt();
            System.out.print(k);
        }

        out.close();
        outputStream.close();
       // localFileSystem.delete(testPath,false);
    }
    }
