import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;

import java.io.IOException;

public class LocalFSTest {
    public static void main(String[] args) throws IOException {
        String parent=System.getProperty("user.home").concat("/hair-loss/app/RocksDB/");
        String child="/test";
        Path testPath=new Path(parent,child);
        LocalFileSystem localFileSystem=new LocalFileSystem();
        localFileSystem.delete(testPath,false);
    }
    }
