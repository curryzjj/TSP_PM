package System.FileSystem.ImplFS;

import System.FileSystem.Path;

import java.io.IOException;

public class LocalFileSystemTest {
    String parent=System.getProperty("user.home").concat("/hair-loss/app/RocksDB/");
    String child="/test";
    Path testPath=new Path(parent,child);
    public void testLocalFileSystemCreate() throws IOException {
        LocalFileSystem localFileSystem=new LocalFileSystem();
        localFileSystem.create(testPath);
    }
}
