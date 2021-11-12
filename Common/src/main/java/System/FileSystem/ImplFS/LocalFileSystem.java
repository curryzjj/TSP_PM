package System.FileSystem.ImplFS;

import System.FileSystem.FSDataOutputStream;
import System.FileSystem.FileSystem;
import System.FileSystem.Path;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;


import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

import static System.util.Preconditions.checkNotNull;
import static System.util.Preconditions.checkState;


public class LocalFileSystem extends FileSystem {
    @Override
    public FSDataOutputStream create(Path filePath) throws IOException {
        if (exists(filePath)) {
            throw new FileAlreadyExistsException("File already exists: " + filePath);
        }

        final Path parent = filePath.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }

        final File file = pathToFile(filePath);
        return new LocalDataOutputStream(file);
    }

    @Override
    public boolean delete(final Path f, final boolean recursive) throws IOException {

        final File file = pathToFile(f);
        if (file.isFile()) {
            return file.delete();
        } else if ((!recursive) && file.isDirectory()) {
            File[] containedFiles = file.listFiles();
            if (containedFiles == null) {
                throw new IOException(
                        "Directory "
                                + file.toString()
                                + " does not exist or an I/O error occurred");
            } else if (containedFiles.length != 0) {
                throw new IOException("Directory " + file.toString() + " is not empty");
            }
        }

        return delete(file);
    }
    /**
     * Deletes the given file or directory.
     *
     * @param f the file to be deleted
     * @return <code>true</code> if all files were deleted successfully, <code>false</code>
     *     otherwise
     * @throws IOException thrown if an error occurred while deleting the files/directories
     */
    private boolean delete(final File f) throws IOException {

        if (f.isDirectory()) {
            final File[] files = f.listFiles();
            if (files != null) {
                for (File file : files) {
                    final boolean del = delete(file);
                    if (!del) {
                        return false;
                    }
                }
            }
        } else {
            return f.delete();
        }

        // Now directory is empty
        return f.delete();
    }

    /**
     * Converts the given Path to a File for this file system. If the path is empty, we will return
     * <tt>new File(".")</tt> instead of <tt>new File("")</tt>, since the latter returns
     * <tt>false</tt> for <tt>isDirectory</tt> judgement (See issue
     * https://issues.apache.org/jira/browse/FLINK-18612).
     */
    public File pathToFile(Path path) {
        String localPath = path.getPath();
        checkState(localPath != null, "Cannot convert a null path to File");

        if (localPath.length() == 0) {
            return new File(".");
        }

        return new File(localPath);
    }

    public boolean exists(Path f) throws IOException {
        final File path = pathToFile(f);
        return path.exists();
    }
    /**
     * Recursively creates the directory specified by the provided path.
     *
     * @return <code>true</code>if the directories either already existed or have been created
     *     successfully, <code>false</code> otherwise
     * @throws IOException thrown if an error occurred while creating the directory/directories
     */
    public boolean mkdirs(final Path f) throws IOException {
        checkNotNull(f, "path is null");
        return mkdirsInternal(pathToFile(f));
    }
    private boolean mkdirsInternal(File file) throws IOException {
        if (file.isDirectory()) {
            return true;
        } else if (file.exists() && !file.isDirectory()) {
            // Important: The 'exists()' check above must come before the 'isDirectory()' check to
            //            be safe when multiple parallel instances try to create the directory

            // exists and is not a directory -> is a regular file
            throw new FileAlreadyExistsException(file.getAbsolutePath());
        } else {
            File parent = file.getParentFile();
            return (parent == null || mkdirsInternal(parent))
                    && (file.mkdir() || file.isDirectory());
        }
    }
}
