package System.tools;

import java.util.Random;

public class randomNumberGenerator {
    //[min, max]
    public static int generateRandom(int min, int max) {
        final Random rn = new Random(System.nanoTime());
        int result = rn.nextInt(max - min + 1) + min;
        return result;
    }
}
