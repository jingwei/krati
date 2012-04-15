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

package krati.store.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import krati.io.Serializer;
import krati.util.IndexedIterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

/**
 * AvroStoreJoiner provides the capability of automatically joining records from more than one
 * {@link AvroStore} to create a combined {@link org.apache.avro.generic.GenericRecord}.
 * 
 * @author jwu
 * @since 08/18, 2011
 */
public final class AvroStoreJoiner<K> implements AvroStore<K> {
    private final static Logger _logger = Logger.getLogger(AvroStoreJoiner.class);
    private final String _name;
    private final String _namespace;
    private final Serializer<K> _keySerializer;
    private final Serializer<GenericRecord> _valSerializer;
    private final Map<String, AvroStore<K>> _storeMap;
    private final Schema _schema;
    private AvroStore<K> _master;
    
    public AvroStoreJoiner(String name, String namespace, Map<String, AvroStore<K>> storeMap, Serializer<K> keySerializer) {
        this._name = name;
        this._namespace= namespace;
        this._storeMap = storeMap;
        this._schema = initJoinerSchema();
        this._keySerializer = keySerializer;
        this._valSerializer = new AvroGenericRecordSerializer(_schema);
        
        _logger.info("Schema: " + getSchema());
    }
    
    protected Schema initJoinerSchema() {
        List<String> sourceList = new ArrayList<String>(sources());
        Collections.sort(sourceList);
        
        List<Field> fields = new ArrayList<Field>();
        
        for(String source : sourceList) {
            List<Schema> l = new ArrayList<Schema>();
            l.add(Schema.create(Type.NULL));
            l.add(getSchema(source));
            
            fields.add(new Field(source, Schema.createUnion(l), null, null));
        }
        
        Schema schema = Schema.createRecord(getName(), null, getNamespace(), false);
        schema.setFields(fields);
        return schema;
    }
    
    public String getName() {
        return _name;
    }
    
    public String getNamespace() {
        return _namespace;
    }
    
    public AvroStore<K> getMaster() {
        return _master;
    }
    
    public void setMaster(AvroStore<K> master) {
        this._master = master;
    }
    
    public Set<String> sources() {
        return _storeMap.keySet();
    }
    
    public Iterator<String> sourceIterator() {
        return _storeMap.keySet().iterator();
    }
    
    public AvroStore<K> getStore(String source) {
        return _storeMap.get(source);
    }
    
    public Schema getSchema(String source) {
        AvroStore<K> store = _storeMap.get(source);
        return (store == null) ? null : store.getSchema();
    }
    
    @Override
    public Schema getSchema() {
        return _schema;
    }
    
    @Override
    public Serializer<K> getKeySerializer() {
        return _keySerializer;
    }
    
    @Override
    public Serializer<GenericRecord> getValueSerializer() {
        return _valSerializer;
    }
    
    @Override
    public int capacity() {
        AvroStore<K> store = getMaster();
        if(store != null) {
            return store.capacity();
        }
        throw new UnsupportedOperationException();
    }
    
    @Override
    public GenericRecord get(K key) {
        if(key == null) {
            return null;
        }
        
        GenericData.Record record = new GenericData.Record(_schema);
        
        for(String source : sources()) {
            AvroStore<K> store = getStore(source);
            if(store != null) {
                record.put(source, store.get(key));
            }
        }
        
        return record;
    }
    
    @Override
    public boolean put(K key, GenericRecord value) throws Exception {
        if(key == null) {
            return false;
        }
        
        if(value == null) {
            return delete(key);
        }
        
        boolean ret = false;
        for(String source : sources()) {
            AvroStore<K> store = getStore(source);
            if(store != null) {
                GenericRecord record = (GenericRecord)value.get(source);
                if(record == null) {
                    if(store.delete(key)) {
                        ret = true;
                    }
                } else {
                    if(store.put(key, record)) {
                        ret = true;
                    }
                }
            }
        }
        
        return ret;
    }
    
    @Override
    public boolean delete(K key) throws Exception {
        if(key == null) {
            return false;
        }
        
        boolean ret = false;
        for(String source : sources()) {
            AvroStore<K> store = getStore(source);
            if(store != null) {
                if(store.delete(key)) {
                    ret = true;
                }
            }
        }
        
        return ret;
    }
    
    @Override
    public IndexedIterator<Entry<K, GenericRecord>> iterator() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public IndexedIterator<K> keyIterator() {
        AvroStore<K> store = getMaster();
        if(store != null) {
            return store.keyIterator();
        }
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void persist() throws IOException {
        for(String source : sources()) {
            AvroStore<K> store = getStore(source);
            if(store != null) {
                try {
                    store.persist();
                } catch(IOException ioe) {
                    _logger.error("Failed to persist " + source, ioe);
                }
            }
        }
    }
    
    @Override
    public void sync() throws IOException {
        for(String source : sources()) {
            AvroStore<K> store = getStore(source);
            if(store != null) {
                try {
                    store.sync();
                } catch(IOException ioe) {
                    _logger.error("Failed to sync " + source, ioe);
                }
            }
        }
    }
    
    /**
     * Unsupported operation. 
     */
    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Unsupported operation. 
     */
    @Override
    public void open() throws IOException {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Unsupported operation. 
     */
    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Unsupported operation. 
     */
    @Override
    public void clear() throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public byte[] getBytes(K key) {
        GenericRecord record = get(key);
        return record == null ? null : _valSerializer.serialize(record);
    }
    
    @Override
    public byte[] getBytes(byte[] keyBytes) {
        GenericRecord record = get(_keySerializer.deserialize(keyBytes));
        return record == null ? null : _valSerializer.serialize(record);
    }
}
