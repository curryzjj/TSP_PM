package engine.table.datatype.serialize;

import System.FileSystem.DataIO.DataOutputView;

import java.io.*;

public final class Serialize {
    public static byte[] serializeObject(Object o) throws IOException{
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        }
    }
    public static void writeSerializeObject(OutputStream out,Object o) throws IOException{
        ObjectOutputStream oos =
                out instanceof ObjectOutputStream
                        ? (ObjectOutputStream) out
                        : new ObjectOutputStream(out);
        oos.writeObject(o);
    }
    public static void writeSerializedKV(byte[] record, DataOutputView target) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("The record must not be null.");
        }
        final int len = record.length;
        target.writeInt(len);
        target.write(record);
    }
    public static byte[] serializeKey(){
return null;
    }

    public static Object cloneObject(Object o) throws IOException, ClassNotFoundException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        }
    }
}
