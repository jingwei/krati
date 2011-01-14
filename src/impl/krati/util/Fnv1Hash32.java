package krati.util;

/**
 * Fnv1Hash32 (Taken from http://www.isthe.com/chongo/tech/comp/fnv)
 * 
 * FNV-1
 * <code>
 *   hash = basis
 *   for each octet_of_data to be hashed
 *       hash = hash * FNV_prime
 *       hash = hash xor octet_of_data
 *   return hash
 * </code>
 * 
 * FNV-1a
 * <code>
 *   hash = basis
 *   for each octet_of_data to be hashed
 *       hash = hash xor octet_of_data
 *       hash = hash * FNV_prime
 *   return hash
 * </code>
 * 
 * @author jwu
 * 01/12, 2011
 */
public class Fnv1Hash32 implements HashFunction<byte[]> {
    public static final long BITS_MASK = 0xffffffffL; 
    public static final long FNV_BASIS = 0x811c9dc5L;
    public static final long FNV_PRIME = (1 << 24) + 0x193;
    
    @Override
    public final long hash(byte[] key) {
        long hash = FNV_BASIS;
        for(int i = 0; i < key.length; i++) {
            hash *= FNV_PRIME;
            hash &= BITS_MASK;
            hash ^= 0xFF & key[i];
        }
        
        return (hash == HashFunction.NON_HASH_CODE) ? Integer.MAX_VALUE : hash;
    }
}
