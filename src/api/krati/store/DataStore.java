package krati.store;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Key Value Store.
 * 
 * @author jwu
 *
 * @param <K> key
 * @param <V> value
 */
public interface DataStore<K, V> extends Iterable<Entry<K, V>> {
    
    public V get(K key);
    
    public boolean put(K key, V value) throws Exception;
    
    public boolean delete(K key) throws Exception;
    
    public void sync() throws IOException;
    
    public void persist() throws IOException;
    
    public void clear() throws IOException;
    
    public Iterator<K> keyIterator();
}
