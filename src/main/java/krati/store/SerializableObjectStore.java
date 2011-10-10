package krati.store;

import java.io.IOException;
import java.util.Map.Entry;

import krati.io.Serializer;
import krati.util.IndexedIterator;

/**
 * SerializableObjectStore is a key-value store for serializable objects.
 * This store requires that both key and value be serializable objects.
 * 
 * <p>
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of SerializableObjectStore for a given data store.
 *    2. There is one and only one thread is calling put and delete methods at any given time. 
 * </pre>
 * 
 * <p>
 * However, if the underlying data store is thread safe, SerializableObjectStore becomes thread-safe automatically. 
 * 
 * <p>
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @param <K> Key
 * @param <V> Value
 * @author jwu
 * 
 * <p>
 * 06/04, 2011 - Added support for Closeable <br/>
 */
public class SerializableObjectStore<K, V> implements ObjectStore<K, V> {
    protected final DataStore<byte[], byte[]> _store;
    protected final Serializer<K> _keySerializer;
    protected final Serializer<V> _valSerializer;
    
    /**
     * Constructs a key-value store for serializable objects.
     * 
     * @param store
     *            the underlying data store for serializable objects.
     * @param keySerializer
     *            the object serializer to serialize/de-serialize keys.
     * @param valueSerializer
     *            the object serializer to serialize/de-serialize values.
     */
    public SerializableObjectStore(DataStore<byte[], byte[]> store, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this._store = store;
        this._keySerializer = keySerializer;
        this._valSerializer = valueSerializer;
    }
    
    /**
     * @return the underlying data store.
     */
    public final DataStore<byte[], byte[]> getStore() {
        return _store;
    }
    
    /**
     * @return the key serializer.
     */
    public final Serializer<K> getKeySerializer() {
        return _keySerializer;
    }
    
    /**
     * @return the value serializer.
     */
    public final Serializer<V> getValueSerializer() {
        return _valSerializer;
    }
    
    /**
     * @return the store capacity.
     */
    @Override
    public final int capacity() {
        return _store.capacity();
    }

    /**
     * Gets an object based on its key from the store.
     * 
     * @param key
     *            the retrieving key for a serializable object in the store.
     * @return the retrieved object.
     */
    @Override
    public V get(K key) {
        if(key == null) {
            return null;
        }
        
        byte[] bytes = _store.get(_keySerializer.serialize(key));
        return bytes == null ? null : _valSerializer.deserialize(bytes);
    }
    
    /**
     * Gets an object in the form of byte array from the store.
     * 
     * @param key
     *            the retrieving key.
     * @return the retrieved object in raw bytes.
     */
    @Override
    public byte[] getBytes(K key) {
        if(key == null) {
            return null;
        }
        
        return _store.get(_keySerializer.serialize(key));
    }
    
    /**
     * Gets an object in the form of byte array from the store.
     * 
     * @param keyBytes
     *            the retrieving key in raw bytes.
     * @return the retrieved object in raw bytes.
     */
    @Override
    public byte[] getBytes(byte[] keyBytes) {
        return _store.get(keyBytes);
    }
    
    /**
     * Puts an serializable object into the store.
     * 
     * @param key
     *            the object key.
     * @param value
     *            the serializable object.
     * @return true if the put operation is succeeds.
     * @throws NullPointerException if <code>key</code> is null.
     * @throws Exception if this operation cannot be completed successfully.
     */
    @Override
    public boolean put(K key, V value) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        if(value == null) {
            return _store.delete(_keySerializer.serialize(key));
        } else {
            return _store.put(_keySerializer.serialize(key), _valSerializer.serialize(value));
        }
    }
    
    /**
     * Deletes an object from the store based on its key.
     * 
     * @param key
     *            the object key.
     * @return true if the delete operation succeeds.
     * @throws NullPointerException if <code>key</code> is null.
     * @throws Exception if this operation cannot be completed successfully.
     */
    @Override
    public boolean delete(K key) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        return _store.delete(_keySerializer.serialize(key));
    }
    
    /**
     * Sync changes to this object store for persistency.
     * 
     * @throws IOException
     */
    @Override
    public void sync() throws IOException {
        _store.sync();
    }
    
    /**
     * Persists this object store.
     * 
     * @throws IOException
     */
    @Override
    public void persist() throws IOException {
        _store.persist();
    }
    
    @Override
    public IndexedIterator<K> keyIterator() {
        if(_store.isOpen()) {
            return new ObjectStoreKeyIterator<K>(_store.keyIterator(), _keySerializer);
        }
        
        throw new StoreClosedException();
    }
    
    @Override
    public IndexedIterator<Entry<K, V>> iterator() {
        if(_store.isOpen()) {
            return new ObjectStoreIterator<K, V>(_store.iterator(), _keySerializer, _valSerializer);
        }
        
        throw new StoreClosedException();
    }
    
    @Override
    public boolean isOpen() {
        return _store.isOpen();
    }
    
    @Override
    public void open() throws IOException {
        _store.open();
    }
    
    @Override
    public void close() throws IOException {
        _store.close();
    }
    
    /**
     * Clears this object store by removing all the persisted data permanently.
     * 
     * @throws IOException if this operation cannot be completed successfully.
     */
    public void clear() throws IOException {
        _store.clear();
    }
}
