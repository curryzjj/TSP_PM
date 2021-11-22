package System.FileSystem.DataIO;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;


public class DataInputViewStreamWrapper extends DataInputStream implements DataInputView {
    public DataInputViewStreamWrapper(InputStream in) {
        super(in);
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {

    }
}
