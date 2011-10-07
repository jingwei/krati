package krati.store;

import java.util.AbstractMap;
import java.util.Map.Entry;

import krati.io.Serializer;
import krati.util.IndexedIterator;

/**
 * ObjectStoreIterator
 * 
 * @author  jwu
 * @since   0.3.5
 * @version 0.4.2
 * 
 * <p>
 * 08/10, 2011 - Implemented IndexedIterator <br/>
 */
public class ObjectStoreIterator<K, V> implements IndexedIterator<Entry<K, V>>{
    private final IndexedIterator<Entry<byte[], byte[]>> _rawIterator;
    private final Serializer<K> _keySerializer;
    private final Serializer<V> _valSerializer;
    
    public ObjectStoreIterator(IndexedIterator<Entry<byte[], byte[]>> rawKeyIterator,
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
    
    @Override
    public int index() {
        return _rawIterator.index();
    }
    
    @Override
    public void reset(int indexStart) {
        _rawIterator.reset(indexStart);
    }
}
