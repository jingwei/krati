package krati.sos;

import krati.store.DataStore;

/**
 * ObjectStore
 * 
 * @author jwu
 *
 * @param <K> Key
 * @param <V> Value
 */
public interface ObjectStore<K, V> extends DataStore<K, V> {
    
    /**
     * Gets an object in the form of byte array from the store.
     * 
     * @param key  the retrieving key. 
     * @return the retrieved object in raw bytes.
     */
    public byte[] getBytes(K key);
    
    /**
     * Gets an object in the form of byte array from the store.
     * 
     * @param keyBytes  the retrieving key in raw bytes. 
     * @return the retrieved object in raw bytes.
     */
    public byte[] getBytes(byte[] keyBytes);
    
}
