package engine.table.datatype.serialize;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Deserialize {
    public static <T> T Deserialize2TableRecord(byte[] value, ClassLoader cl) throws IOException, ClassNotFoundException {
        ObjectInputStream oois =new ClassLoaderObjectInputStream(new ByteArrayInputStream(value),cl);
        Thread.currentThread().setContextClassLoader(cl);
        return (T) oois.readObject();
    }
}
