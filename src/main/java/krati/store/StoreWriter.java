package krati.store;

/**
 * StoreWriter
 * 
 * @author jwu
 * @since 09/15, 2011
 */
public interface StoreWriter<K, V> {
    
    /**
     * Maps the specified <code>key</code> to the specified <code>value</code> in this store.
     * 
     * @param key   - the key
     * @param value - the value
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed for any reasons.
     */
    public boolean put(K key, V value) throws Exception;
    
    /**
     * Removes the specified <code>key</code> from this store.
     * 
     * @param key   - the key
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed for any reasons.
     */
    public boolean delete(K key) throws Exception;
    
}
