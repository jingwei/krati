package test.util;

import java.util.Random;

/**
 * RandomBytes
 * 
 * @author jwu
 * 06/01, 2011
 * 
 */
public class RandomBytes {
    private final static Random _rand = new Random();

    public static byte[] getBytes() {
        byte[] bytes = new byte[_rand.nextInt(4096)];
        _rand.nextBytes(bytes);
        return bytes;
    }
    
    public static byte[] getBytes(int length) {
        byte[] bytes = new byte[length];
        _rand.nextBytes(bytes);
        return bytes;
    }
}
