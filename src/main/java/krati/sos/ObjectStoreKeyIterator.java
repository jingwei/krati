package krati.sos;

import java.util.Iterator;

/**
 * ObjectStoreKeyIterator
 * 
 * @author jwu
 * 
 */
public class ObjectStoreKeyIterator<K> implements Iterator<K> {
    private final Iterator<byte[]> _rawKeyIterator;
    private final ObjectSerializer<K> _keySerializer;
    
    public ObjectStoreKeyIterator(Iterator<byte[]> rawKeyIterator, ObjectSerializer<K> keySerializer) {
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
        return (rawKey == null) ? null : _keySerializer.construct(rawKey);
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
