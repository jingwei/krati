package krati.retention;

import krati.Persistable;

/**
 * RetentionStoreWriter
 * 
 * @param <K> Key
 * @param <V> Value
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/16, 2011 - Created <br/>
 */
public interface RetentionStoreWriter<K, V> extends Persistable {
    
    /**
     * @return the data source of this RetentionStoreWriter.
     */
    public String getSource();
    
    /**
     * Puts a key-value pair into the underlying store.
     * 
     * @param key   - the key
     * @param value - the value
     * @param scn   - the System Change Number (SCN) representing an ever-increasing update order.
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception
     */
    public boolean put(K key, V value, long scn) throws Exception;
    
    /**
     * Deletes a key-value pair from the underlying store based on a given key.
     * 
     * @param key   - the key
     * @param scn   - the System Change Number (SCN) representing an ever-increasing update order.
     * @return <code>true</code> if this store is changed as a result of this operation.
     *         Otherwise, <cod>false</code>.
     * @throws Exception
     */
    public boolean delete(K key, long scn) throws Exception;
}
