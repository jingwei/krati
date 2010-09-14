package krati.util;

/**
 * Taken from http://www.isthe.com/chongo/tech/comp/fnv and Voldemort (voldemort.utils.FnvHashFunction)
 * 
 * hash = basis for each octet_of_data to be hashed hash = hash * FNV_prime hash
 * = hash xor octet_of_data return hash
 * 
 */
public final class FnvHashFunction implements HashFunction<byte[]> {
    private static final long FNV_BASIS = 0x811c9dc5;
    private static final long FNV_PRIME = (1 << 24) + 0x193;
    
    @Override
    public final long hash(byte[] buffer) {
        long hash = FNV_BASIS;
        for(int i = 0; i < buffer.length; i++) {
            hash ^= 0xFF & buffer[i];
            hash *= FNV_PRIME;
        }
        
        return (hash == HashFunction.NON_HASH_CODE) ? HashFunction.MAX_HASH_CODE : hash;
    }
}
