package krati.retention;

import java.util.Map;

import krati.store.StoreReader;

/**
 * RetentionStoreReader
 * 
 * @param <K> Key
 * @param <V> Value
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/23, 2011 - Created <br/>
 * 09/21, 2011 - Added interface StoreReader <br/>
 */
public interface RetentionStoreReader<K, V> extends RetentionClient<K>, StoreReader<K, V> {
    
    /**
     * @return the data source of this RetentionStoreReader.
     */
    public String getSource();
    
    /**
     * Gets a value from the underlying data store.
     * 
     * @param key - the key
     * @return a value associated with the <tt>key</tt>.
     * @throws Exception
     */
    @Override
    public V get(K key) throws Exception;
    
    /**
     * Gets a number of value events starting from a give position in the Retention.
     * The number of events is determined internally by the Retention and it is
     * up to the batch size.   
     * 
     * @param pos - the retention position from where events will be read
     * @param map - the result map (keys to value events) to fill in 
     * @return the next position from where new events will be read.
     */
    public Position get(Position pos, Map<K, Event<V>> map);
}
