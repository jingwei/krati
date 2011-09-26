package krati.store;

/**
 * VersionedStoreWriter
 * 
 * @author jwu
 * @since 09/21, 2011
 */
public interface VersionedStoreWriter<K, V> {

    /**
     * Maps the specified <code>key</code> to the specified <code>value</code> in this store.
     * 
     * @param key   - the key
     * @param value - the value
     * @param scn   - the System Change Number (SCN) representing an ever-increasing update order.
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed for any reasons.
     */
    public boolean put(K key, V value, long scn) throws Exception;
    
    /**
     * Removes the specified <code>key</code> from this store.
     * 
     * @param key   - the key
     * @param scn   - the System Change Number (SCN) representing an ever-increasing update order.
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception if this operation cannot be completed for any reasons.
     */
    public boolean delete(K key, long scn) throws Exception;
}
