package krati.util;

/**
 * Fnv1aHash64 (Taken from http://www.isthe.com/chongo/tech/comp/fnv)
 * 
 * @author jwu
 * 01/12, 2011
 */
public class Fnv1aHash64 implements HashFunction<byte[]> {
    
    @Override
    public final long hash(byte[] key) {
        long hash = Fnv1Hash64.FNV_BASIS;
        for(int i = 0; i < key.length; i++) {
            hash ^= 0xFF & key[i];
            hash *= Fnv1Hash64.FNV_PRIME;
        }
        
        return (hash == HashFunction.NON_HASH_CODE) ? HashFunction.MAX_HASH_CODE : hash;
    }
}
