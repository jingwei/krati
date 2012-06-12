/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.store;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Map.Entry;

import krati.io.Serializer;
import krati.io.serializer.IntSerializer;
import krati.util.IndexedIterator;

/**
 * SerializableObjectArray is an array store for serializable objects.
 * This store requires that values be serializable objects.
 * 
 * <p>
 * This class is not thread-safe by design. It is expected that the conditions below hold within one JVM.
 * <pre>
 *    1. There is one and only one instance of SerializableObjectArray for a given array store.
 *    2. There is one and only one thread is calling put and delete methods at any given time. 
 * </pre>
 * 
 * <p>
 * However, if the underlying array store is thread safe, SerializableObjectArray becomes thread-safe automatically. 
 * 
 * <p>
 * It is expected that this class is used in the case of multiple readers and single writer.
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class SerializableObjectArray<V> implements ObjectStore<Integer, V> {
    protected final ArrayStore _store;
    protected final Serializer<Integer> _keySerializer;
    protected final Serializer<V> _valSerializer;
    
    /**
     * Creates a new instance of <code>SerializableObjectArray</code>.
     * 
     * @param store           - the underlying array store for serializable objects.
     * @param valueSerializer - the object serializer
     */
    public SerializableObjectArray(ArrayStore store, Serializer<V> valueSerializer) {
        this(store, new IntSerializer(ByteOrder.BIG_ENDIAN), valueSerializer);
    }
    
    /**
     * Creates a new instance of <code>SerializableObjectArray</code>.
     * 
     * @param store           - the underlying array store for serializable objects
     * @param keySerializer   - the integer key serializer
     * @param valueSerializer - the object serializer
     */
    public SerializableObjectArray(ArrayStore store, Serializer<Integer> keySerializer, Serializer<V> valueSerializer) {
        this._store = store;
        this._keySerializer = keySerializer;
        this._valSerializer = valueSerializer;
    }
    
    /**
     * @return the next system change number for put/delete operations.
     */
    protected long nextScn() {
        return System.currentTimeMillis();
    }
    
    /**
     * @return the underlying array store.
     */
    public final ArrayStore getStore() {
        return _store;
    }
    
    /**
     * @return the key (integer) serializer.
     */
    public final Serializer<Integer> getKeySerializer() {
        return _keySerializer;
    }
    
    /**
     * @return the value serializer.
     */
    public final Serializer<V> getValueSerializer() {
        return _valSerializer;
    }
    
    @Override
    public final int capacity() {
        return _store.capacity();
    }
    
    public V get(int index) {
        byte[] bytes = _store.get(index);
        return bytes == null ? null : _valSerializer.deserialize(bytes);
    }
    
    @Override
    public int getLength(Integer key) {
        if(key == null) {
            return -1;
        }
        
        int index = key.intValue();
        return _store.getLength(index);
    }
    
    @Override
    public V get(Integer key) {
        if(key == null) {
            return null;
        }
        
        int index = key.intValue();
        byte[] bytes = _store.get(index);
        return bytes == null ? null : _valSerializer.deserialize(bytes);
    }
    
    public byte[] getBytes(int index) {
        return _store.get(index);
    }
    
    @Override
    public byte[] getBytes(Integer key) {
        if(key == null) {
            return null;
        }
        
        return _store.get(key.intValue());
    }
    
    @Override
    public byte[] getBytes(byte[] keyBytes) {
        if(keyBytes == null) {
            return null;
        }
        
        int index = _keySerializer.deserialize(keyBytes);
        return _store.get(index);
    }
    
    public boolean set(int index, V value) throws Exception {
        if(value == null) {
            if(_store.hasIndex(index)) {
                _store.delete(index, nextScn());
                return true;
            } else {
                return false;
            }
        } else {
            byte[] bytes = _valSerializer.serialize(value);
            _store.set(index, bytes, nextScn());
            return true;
        }
    }
    
    public boolean put(int index, V value) throws Exception {
        if(value == null) {
            if(_store.hasIndex(index)) {
                _store.delete(index, nextScn());
                return true;
            } else {
                return false;
            }
        } else {
            byte[] bytes = _valSerializer.serialize(value);
            _store.set(index, bytes, nextScn());
            return true;
        }
    }
    
    @Override
    public boolean put(Integer key, V value) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        return set(key.intValue(), value);
    }
    
    public boolean delete(int index) throws Exception {
        if(_store.hasIndex(index)) {
            _store.delete(index, nextScn());
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public boolean delete(Integer key) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        return delete(key.intValue());
    }
    
    @Override
    public IndexedIterator<Integer> keyIterator() {
        if(_store.isOpen()) {
            return new ArrayStoreIndexIterator(_store);
        }
        
        throw new StoreClosedException();
    }
    
    @Override
    public IndexedIterator<Entry<Integer, V>> iterator() {
        if(_store.isOpen()) {
            return new ObjectArrayIterator<V>(_store, _valSerializer);
        }
        
        return null;
    }
    
    @Override
    public void persist() throws IOException {
        _store.persist();
    }
    
    @Override
    public void sync() throws IOException {
        _store.sync();
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
     * Clears this object array by removing all the persisted data permanently.
     * 
     * @throws IOException if this operation cannot be completed successfully.
     */
    @Override
    public void clear() throws IOException {
        _store.clear();
    }
}
