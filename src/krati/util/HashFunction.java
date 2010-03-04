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

}
