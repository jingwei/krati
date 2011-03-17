package krati.sos;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * ObjectStoreIterator
 * 
 * @author jwu
 * 
 */
public class ObjectStoreIterator<K, V> implements Iterator<Entry<K, V>>{
    private final Iterator<Entry<byte[], byte[]>> _rawIterator;
    private final ObjectSerializer<K> _keySerializer;
    private final ObjectSerializer<V> _valSerializer;
    
    public ObjectStoreIterator(Iterator<Entry<byte[], byte[]>> rawKeyIterator,
                               ObjectSerializer<K> keySerializer,
                               ObjectSerializer<V> valSerializer) {
        this._rawIterator = rawKeyIterator;
        this._keySerializer = keySerializer;
        this._valSerializer = valSerializer;
    }
    
    @Override
    public boolean hasNext() {
        return _rawIterator.hasNext();
    }
    
    @Override
    public Entry<K, V> next() {
        Entry<byte[], byte[]> entry = _rawIterator.next();
        return (entry == null) ? null :
            new AbstractMap.SimpleEntry<K, V>(
                _keySerializer.construct(entry.getKey()), _valSerializer.construct(entry.getValue()));
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
