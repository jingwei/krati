package krati.store;

import krati.io.Serializer;
import krati.util.IndexedIterator;

/**
 * ObjectStoreKeyIterator
 * 
 * @author  jwu
 * @since   0.3.5
 * @version 0.4.2
 * 
 * <p>
 * 08/10, 2011 - Implemented IndexedIterator <br/>
 */
public class ObjectStoreKeyIterator<K> implements IndexedIterator<K> {
    private final IndexedIterator<byte[]> _rawKeyIterator;
    private final Serializer<K> _keySerializer;
    
    public ObjectStoreKeyIterator(IndexedIterator<byte[]> rawKeyIterator, Serializer<K> keySerializer) {
        this._rawKeyIterator = rawKeyIterator;
        this._keySerializer = keySerializer;
    }
    
    @Override
    public boolean hasNext() {
        return _rawKeyIterator.hasNext();
    }
    
    @Override
    public K next() {
        byte[] rawKey = _rawKeyIterator.next();
        return (rawKey == null) ? null : _keySerializer.deserialize(rawKey);
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public int index() {
        return _rawKeyIterator.index();
    }
    
    @Override
    public void reset(int indexStart) {
        _rawKeyIterator.reset(indexStart);
    }
}
