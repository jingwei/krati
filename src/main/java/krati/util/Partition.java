package krati.util;

/**
 * Partition
 * 
 * @author jwu
 * 07/11, 2011
 * 
 * @param <K> Key
 */
public interface Partition<K> {
    
    /**
     * Tests if this Partition contains a key.
     * 
     * @param key
     * @return <code>true</code> if this Partition contains the key.
     */
    public boolean contains(K key);
    
    /**
     * @return the capacity of this Partition.
     */
    public int capacity();
}
