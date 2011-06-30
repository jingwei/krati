package krati.sos;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;

import krati.io.Serializer;

/**
 * ObjectStoreIterator
 * 
 * @author jwu
 * 
 */
public class ObjectStoreIterator<K, V> implements Iterator<Entry<K, V>>{
    private final Iterator<Entry<byte[], byte[]>> _rawIterator;
    private final Serializer<K> _keySerializer;
    private final Serializer<V> _valSerializer;
    
    public ObjectStoreIterator(Iterator<Entry<byte[], byte[]>> rawKeyIterator,
                               Serializer<K> keySerializer,
                               Serializer<V> valSerializer) {
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
                _keySerializer.deserialize(entry.getKey()), _valSerializer.deserialize(entry.getValue()));
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
