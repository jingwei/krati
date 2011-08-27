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
     * @throws Exception
     */
    public boolean put(K key, V value, long scn) throws Exception;
    
    /**
     * Delete a key-value pair from the underlying store based a given key.
     * 
     * @param key   - the key
     * @param scn   - the System Change Number (SCN) representing an ever-increasing update order.
     * @throws Exception
     */
    public boolean delete(K key, long scn) throws Exception;
}
