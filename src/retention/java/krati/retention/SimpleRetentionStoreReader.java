package krati.retention;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import krati.retention.clock.Clock;
import krati.store.DataStore;

/**
 * SimpleRetentionStoreReader
 * 
 * @param <K> Key
 * @param <V> Value
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/23, 2011 - Created <br/>
 */
public class SimpleRetentionStoreReader<K, V> implements RetentionStoreReader<K, V> {
    private final String _source;
    private final Retention<K> _retention;
    private final DataStore<K, V> _store;
    
    public SimpleRetentionStoreReader(String source, Retention<K> retention, DataStore<K, V> store) {
        this._source = source;
        this._retention = retention;
        this._store = store;
    }
    
    public final DataStore<K, V> getStore() {
        return _store;
    }
    
    public final Retention<K> getRetention() {
        return _retention;
    }
    
    @Override
    public final String getSource() {
        return _source;
    }
    
    @Override
    public Position getPosition() {
        return _retention.getPosition();
    }
    
    @Override
    public Position getPosition(Clock sinceClock) {
        return _retention.getPosition(sinceClock);
    }
    
    @Override
    public V get(K key) throws Exception {
        return key == null ? null : _store.get(key);
    }
    
    @Override
    public Position get(Position pos, Map<K, Event<V>> map) {
        ArrayList<Event<K>> list = new ArrayList<Event<K>>(1000);
        Position nextPos = get(pos, list);
        
        for(Event<K> e : list) {
            K key = e.getValue();
            if(key != null) {
                V value = _store.get(key);
                map.put(key, new SimpleEvent<V>(value, e.getClock()));
            }
        }
        
        return nextPos;
    }
    
    @Override
    public Position get(Position pos, List<Event<K>> list) {
        return _retention.get(pos, list);
    }
}
