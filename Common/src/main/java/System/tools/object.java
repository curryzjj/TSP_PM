package System.tools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

public abstract class object implements Serializable {
    final int MB = 1024 * 1024;
    final int KB = 1024;
    final String alphabet = "0123456789ABCDE";
    final int N = alphabet.length();

    Random r = new Random();

    public abstract ArrayList<object> create_myObjectList(int size_state);

    public abstract Object getValue();
}
