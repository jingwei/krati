package krati.util;

import java.util.Arrays;

/**
 * Bytes defines various byte array utilities. 
 * 
 * @author jwu
 * @since 08/18, 2012
 */
public class Bytes {
    /**
     * The number of bytes of an Integer value.
     */
    public final static int NUM_BYTES_IN_INT = 4;
    
    /**
     * The number of bytes of a long value.
     */
    public final static int NUM_BYTES_IN_LONG = 8;
    
    /**
     * The number of bytes of a short value.
     */
    public final static int NUM_BYTES_IN_SHORT = 2;
    
    /**
     * Returns <code>true</code> if the two specified arrays of bytes are equal to one another.
     * 
     * @param src - the source array to be tested for equality
     * @param dst - the destination array to be tested for equality
     * @return <code>true</code> if the two arrays are equal to one another.
     * 
     * @throws NullPointerException
     */
    public static boolean equals(byte[] src, byte[] dst) {
        return Arrays.equals(src, dst);
    }
    
    /**
     * Returns <code>true</code> if the two specified arrays of bytes are equal to one another.
     * 
     * @param src       - the source array to be tested for equality
     * @param dst       - the destination array to be tested for equality
     * @param dstOffset - the destination array offset
     * @return <code>true</code> if the two arrays are equal to one another w.r.t. the specified <code>dstOffset</code>.
     * 
     * @throws NullPointerException
     * @throws ArrayIndexOutOfBoundsException
     */
    public static boolean equals(byte[] src, byte[] dst, int dstOffset) {
        int numBytes = src.length;
        
        for(int i = 0; i < numBytes; i++) {
            if(src[i] != dst[dstOffset + i])
                return false;
        }
        
        return true;
    }
    
    /**
     * Returns <code>true</code> if the two specified arrays of bytes are equal to one another.
     * 
     * @param src       - the source array to be tested for equality.
     *                    It is expected that the source array have the specified <code>numBytes</code> as its length.
     * @param dst       - the destination array to be tested for equality
     * @param dstOffset - the destination array offset
     * @param numBytes  - the number of bytes to compare 
     * @return <code>true</code> if the two arrays are equal to one another w.r.t. the specified <code>dstOffset</code>.
     * 
     * @throws NullPointerException
     * @throws ArrayIndexOutOfBoundsException
     */
    public static boolean equals(byte[] src, byte[] dst, int dstOffset, int numBytes) {
        if(src.length == numBytes) {
            for(int i = 0; i < numBytes; i++) {
                if(src[i] != dst[dstOffset + i])
                    return false;
            }
            return true;
        }
        
        return false;
    }
    
    /**
     * Returns <code>true</code> if the two specified arrays of bytes are equal to one another.
     * 
     * @param src       - the source array to be tested for equality
     * @param srcOffset - the source array offset
     * @param dst       - the destination array to be tested for equality
     * @param dstOffset - the destination array offset
     * @param numBytes  - the number of bytes to compare
     * @return <code>true</code> if the two arrays are equal to one another w.r.t. the specified <code>dstOffset</code>.
     * 
     * @throws NullPointerException
     * @throws ArrayIndexOutOfBoundsException
     */
    public static boolean equals(byte[] src, int srcOffset, byte[] dst, int dstOffset, int numBytes) {
        for(int i = 0; i < numBytes; i++) {
            if(src[srcOffset + i] != dst[dstOffset + i])
                return false;
        }
        
        return true;
    }
}
