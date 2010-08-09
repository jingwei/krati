package krati.util;

/**
 * HashFunction
 * 
 * A hash function for mapping bytes to long
 * 
 * @author jwu
 *
 */
public interface HashFunction<K> {
    public long hash(K key);
    public static final long NON_HASH_CODE = 0;
    public static final long MIN_HASH_CODE = Long.MIN_VALUE;
    public static final long MAX_HASH_CODE = Long.MAX_VALUE;
}
