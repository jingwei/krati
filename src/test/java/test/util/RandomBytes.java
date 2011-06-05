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
    
    /**
     * @return a random byte array of size from 0 byte to 4096 bytes. 
     */
    public static byte[] getBytes() {
        byte[] bytes = new byte[_rand.nextInt(4096)];
        _rand.nextBytes(bytes);
        return bytes;
    }
    
    /**
     * Gets a fixed-length random byte array.
     * 
     * @param length - byte array length
     * @return a fixed-length random byte array.
     */
    public static byte[] getBytes(int length) {
        byte[] bytes = new byte[length];
        _rand.nextBytes(bytes);
        return bytes;
    }
}
