package krati.util;

/**
 * Numbers
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class Numbers {
    
    public static byte[] intBytesLE(int val) {
        byte[] buffer = new byte[4];
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
        buffer[2] = (byte) (val >> 16);
        buffer[3] = (byte) (val >> 24);
        return buffer;
    }
    
    public static byte[] intBytesBE(int val) {
        byte[] buffer = new byte[4];
        buffer[3] = (byte) (val);
        buffer[2] = (byte) (val >> 8);
        buffer[1] = (byte) (val >> 16);
        buffer[0] = (byte) (val >> 24);
        return buffer;
    }
    
    public static byte[] longBytesLE(long val) {
        byte[] buffer = new byte[8];
        buffer[0] = (byte) (val);
        buffer[1] = (byte) (val >> 8);
        buffer[2] = (byte) (val >> 16);
        buffer[3] = (byte) (val >> 24);
        buffer[4] = (byte) (val >> 32);
        buffer[5] = (byte) (val >> 40);
        buffer[6] = (byte) (val >> 48);
        buffer[7] = (byte) (val >> 56);
        return buffer;
    }
    
    public static byte[] longBytesBE(long val) {
        byte[] buffer = new byte[8];
        buffer[7] = (byte) (val);
        buffer[6] = (byte) (val >> 8);
        buffer[5] = (byte) (val >> 16);
        buffer[4] = (byte) (val >> 24);
        buffer[3] = (byte) (val >> 32);
        buffer[2] = (byte) (val >> 40);
        buffer[1] = (byte) (val >> 48);
        buffer[0] = (byte) (val >> 56);
        return buffer;
    }
    
    public static int intValueLE(byte[] bytes) {
        return (((bytes[3] & 0xFF) << 24) |
                ((bytes[2] & 0xFF) << 16) |
                ((bytes[1] & 0xFF) << 8) |
                ((bytes[0] & 0xFF)));
    }
    
    public static int intValueBE(byte[] bytes) {
        return (((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                ((bytes[3] & 0xFF)));
    }
    
    public static int intValueLE(byte[] bytes, int offset) {
        return (((bytes[3 + offset] & 0xFF) << 24) |
                ((bytes[2 + offset] & 0xFF) << 16) |
                ((bytes[1 + offset] & 0xFF) << 8) |
                ((bytes[0 + offset] & 0xFF)));
    }
    
    public static int intValueBE(byte[] bytes, int offset) {
        return (((bytes[0 + offset] & 0xFF) << 24) |
                ((bytes[1 + offset] & 0xFF) << 16) |
                ((bytes[2 + offset] & 0xFF) << 8) |
                ((bytes[3 + offset] & 0xFF)));
    }
    
    public static long longValueLE(byte[] bytes) {
        return (((long) (bytes[7] & 0xFF) << 56) |
                ((long) (bytes[6] & 0xFF) << 48) |
                ((long) (bytes[5] & 0xFF) << 40) |
                ((long) (bytes[4] & 0xFF) << 32) |
                ((long) (bytes[3] & 0xFF) << 24) |
                ((long) (bytes[2] & 0xFF) << 16) |
                ((long) (bytes[1] & 0xFF) << 8) |
                ((long) (bytes[0] & 0xFF)));
    }
    
    public static long longValueBE(byte[] bytes) {
        return (((long) (bytes[0] & 0xFF) << 56) |
                ((long) (bytes[1] & 0xFF) << 48) |
                ((long) (bytes[2] & 0xFF) << 40) |
                ((long) (bytes[3] & 0xFF) << 32) |
                ((long) (bytes[4] & 0xFF) << 24) |
                ((long) (bytes[5] & 0xFF) << 16) |
                ((long) (bytes[6] & 0xFF) << 8) |
                ((long) (bytes[7] & 0xFF)));
    }
    
    public static long longValueLE(byte[] bytes, int offset) {
        return (((long) (bytes[7 + offset] & 0xFF) << 56) |
                ((long) (bytes[6 + offset] & 0xFF) << 48) |
                ((long) (bytes[5 + offset] & 0xFF) << 40) |
                ((long) (bytes[4 + offset] & 0xFF) << 32) |
                ((long) (bytes[3 + offset] & 0xFF) << 24) |
                ((long) (bytes[2 + offset] & 0xFF) << 16) |
                ((long) (bytes[1 + offset] & 0xFF) << 8) |
                ((long) (bytes[0 + offset] & 0xFF)));
    }
    
    public static long longValueBE(byte[] bytes, int offset) {
        return (((long) (bytes[0 + offset] & 0xFF) << 56) |
                ((long) (bytes[1 + offset] & 0xFF) << 48) |
                ((long) (bytes[2 + offset] & 0xFF) << 40) |
                ((long) (bytes[3 + offset] & 0xFF) << 32) |
                ((long) (bytes[4 + offset] & 0xFF) << 24) |
                ((long) (bytes[5 + offset] & 0xFF) << 16) |
                ((long) (bytes[6 + offset] & 0xFF) << 8) |
                ((long) (bytes[7 + offset] & 0xFF)));
    }
}
