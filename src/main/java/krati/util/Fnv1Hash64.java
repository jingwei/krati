package krati.util;

/**
 * Fnv1Hash64 (Taken from http://www.isthe.com/chongo/tech/comp/fnv)
 * 
 * @author jwu
 * 01/12, 2011
 */
public class Fnv1Hash64 implements HashFunction<byte[]> {
    public static final long FNV_BASIS = 0xcbf29ce484222325L;
    public static final long FNV_PRIME = (1 << 40) + 0x1b3;
    
    @Override
    public final long hash(byte[] key) {
        long hash = FNV_BASIS;
        for(int i = 0; i < key.length; i++) {
            hash *= FNV_PRIME;
            hash ^= 0xFF & key[i];
        }
        
        return (hash == HashFunction.NON_HASH_CODE) ? HashFunction.MAX_HASH_CODE : hash;
    }
}
