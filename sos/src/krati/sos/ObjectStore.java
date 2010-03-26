package krati.sos;

import java.io.IOException;

/**
 * ObjectStore
 * 
 * @author jwu
 *
 * @param <K> Key
 * @param <V> Value
 */
public interface ObjectStore<K, V>
{
    /**
     * Gets an object based on its key from the store.
     * 
     * @param key  the retrieving key for an object in the store. 
     * @return the retrieved object.
     */
    public V get(K key);
    
    /**
     * Puts an object into the store.
     * 
     * @param key    the object key.
     * @param value  the object.
     * @return true if the put operation is succeeds.  
     * @throws Exception
     */
    public boolean put(K key, V value) throws Exception;
    
    /**
     * Deletes an object from the store based on its key.
     * 
     * @param key   the object key.
     * @return true if the delete operation succeeds.
     * @throws Exception
     */
    public boolean delete(K key) throws Exception;
    
    /**
     * Persists this object store.
     * 
     * @throws IOException
     */
    public void persist() throws IOException;
}
