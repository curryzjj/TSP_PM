package System.FileSystem;


import java.io.IOException;

public abstract class FileSystem {
    public abstract FSDataOutputStream create(Path path) throws IOException;
    public abstract boolean delete(Path f, boolean recursive) throws IOException;
}
