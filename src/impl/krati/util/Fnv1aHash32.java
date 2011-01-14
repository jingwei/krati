package krati.util;

/**
 * Fnv1aHash32 (Taken from http://www.isthe.com/chongo/tech/comp/fnv)
 * 
 * @author jwu
 * 01/12, 2011
 */
public class Fnv1aHash32 implements HashFunction<byte[]> {
    
    @Override
    public final long hash(byte[] key) {
        long hash = Fnv1Hash32.FNV_BASIS;
        for(int i = 0; i < key.length; i++) {
            hash ^= 0xFF & key[i];
            hash *= Fnv1Hash32.FNV_PRIME;
            hash &= Fnv1Hash32.BITS_MASK;
        }
        
        return (hash == HashFunction.NON_HASH_CODE) ? Integer.MAX_VALUE : hash;
    }
}
