package System.FileSystem;


import java.io.File;
import java.io.IOException;

public abstract class FileSystem {
    public abstract FSDataOutputStream create(Path path) throws IOException;
    public abstract boolean delete(Path f, boolean recursive) throws IOException;

    public abstract boolean mkdirs(Path parent) throws IOException;

    public abstract File pathToFile(Path current_path);
}
