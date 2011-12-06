package krati.store;

import java.io.IOException;
import java.util.Map.Entry;

import krati.io.Closeable;
import krati.util.IndexedIterator;

/**
 * DataStore defines an interface for associating keys with values.
 * 
 * @param <K> Store key
 * @param <V> Store value
 * 
 * @author jwu
 * 
 * <p>
 * 06/04, 2011 - Added support for Closeable <br/>
 * 12/05, 2011 - Added JavaDoc comment <br/>
 */
public interface DataStore<K, V> extends Iterable<Entry<K, V>>, Closeable {
    
    /**
     * @return the capacity of this store.
     */
    public int capacity();
    
    /**
     * Gets the value to which the specified <code>key</code> is mapped in this store.
     *  
     * @param key - store key
     * @return the value to which the specified <code>key</code> is mapped.
     *         Value <code>null</code> is returned if the <code>key</code> is unknown to this store.
     */
    public V get(K key);
    
    /**
     * Creates a mapping from the specified <code>key</code> to the specified value <code>value</code> in this store.
     * When <code>value</code> is <code>null</code>, this method is equivalent to {@link #delete(Object) delete(K)}.
     * 
     * @param key   - store key
     * @param value - store value.
     * @return <code>true</code> if the operation is completed successfully.
     * @throws Exception if the operation cannot be completed.
     */
    public boolean put(K key, V value) throws Exception;
    
    /**
     * Removes the mapping for the specified <code>key</code> from this store if present.
     *  
     * @param key   - store key
     * @return <code>true</code> if the underlying store is changes as result of this operation.
     * Otherwise, <code>false</code>.
     * 
     * @throws Exception if this operation cannot be completed.
     */
    public boolean delete(K key) throws Exception;
    
    /**
     * Sync changes to this store by updating the underlying indexes directly.
     * 
     * @throws IOException if this operation cannot be completed.
     */
    public void sync() throws IOException;
    
    /**
     * Persist changes to write-ahead log without updating the underlying indexes directly.
     * This operation is generally faster than {@link #sync()}.
     * 
     * @throws IOException if this operation cannot be completed.
     */
    public void persist() throws IOException;
    
    /**
     * Clears this store by removing all mappings from keys to values.
     * 
     * @throws IOException if this operation cannot be completed.
     */
    public void clear() throws IOException;
    
    /**
     * @return the iterator of keys.
     */
    public IndexedIterator<K> keyIterator();
    
    /**
     * @return the iterator of mappings from keys to values.
     */
    @Override
    public IndexedIterator<Entry<K, V>> iterator();
}
