package krati.store;

/**
 * StoreReader
 * 
 * @author jwu
 * @since 09/15, 2011
 */
public interface StoreReader<K, V> {
    
    /**
     * Returns the value to which the specified <code>key</code> is mapped in this store.
     * 
     * @param key - the key
     * @return the value associated with the <code>key</code>,
     *         or <code>null</code> if the <code>key</code> is not known to this store. 
     * @throws Exception if this operation cannot be completed.
     */
    public V get(K key) throws Exception;
    
}
