package System.FileSystem.DataIO;

import System.FileSystem.DataIO.DataInputView;
import System.FileSystem.DataIO.DataOutputView;

import java.io.IOException;

public interface IOReadableWritable {
    /**
     * Writes the object's internal data to the given data output view.
     *
     * @param out the output view to receive the data.
     * @throws IOException thrown if any error occurs while writing to the output stream
     */
    void write(DataOutputView out) throws IOException;

    /**
     * Reads the object's internal data from the given data input view.
     *
     * @param in the input view to read the data from
     * @throws IOException thrown if any error occurs while reading from the input stream
     */
    void read(DataInputView in) throws IOException;
}
