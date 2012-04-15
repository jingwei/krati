/*
 * Copyright (c) 2011 LinkedIn, Inc
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

package krati.store.avro.protocol;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import krati.io.Serializer;
import krati.store.avro.AvroStore;

/**
 * AvroStoreProtocolHandler defines the interface between an {@link AvroStore} and the transport protocol
 * (see {@link krati.store.avro.protocol.Protocols#getProtocol() Protocols}).
 * 
 * @author jwu
 * @since 09/22, 2011
 */
public class AvroStoreProtocolHandler<K> implements StoreProtocolHandler {
    protected final AvroStore<K> _store;
    protected final Serializer<K> _keySerializer;
    protected final Serializer<GenericRecord> _valueSerializer;
    
    public AvroStoreProtocolHandler(AvroStore<K> store) {
        this._store = store;
        this._keySerializer = store.getKeySerializer();
        this._valueSerializer = store.getValueSerializer();
    }
    
    /**
     * @return the underlying {@link krati.store.avro.AvroStore}.
     */
    public final AvroStore<K> getStore() {
        return _store;
    }
    
    @Override
    public String meta(String opt, String key, String value) throws IOException {
        StoreDirective directive = StoreDirective.valueOf(opt);
        
        if(directive == StoreDirective.StoreSync) {
            _store.sync();
            return ProtocolConstants.SUC;
        }
        
        if(directive == StoreDirective.StoreOpen) {
            if(!_store.isOpen()) {
                _store.open();
            }
            return ProtocolConstants.SUC;
        }
        
        if(directive == StoreDirective.StoreClose) {
            if(_store.isOpen()) {
                _store.close();
            }
            return ProtocolConstants.SUC;
        }
        
        if(directive == StoreDirective.StoreInit) {
            return ProtocolConstants.NOP;
        }
        
        // no-operation
        return ProtocolConstants.NOP;
    }
    
    @Override
    public byte[] get(byte[] key) {
        K storeKey = _keySerializer.deserialize(key);
        GenericRecord storeValue = _store.get(storeKey);
        return storeValue == null ? null : _valueSerializer.serialize(storeValue);
    }
    
    @Override
    public boolean put(byte[] key, byte[] value) throws Exception {
        K storeKey = _keySerializer.deserialize(key);
        GenericRecord storeValue = _valueSerializer.deserialize(value);
        return _store.put(storeKey, storeValue);
    }
    
    @Override
    public boolean delete(byte[] key) throws Exception {
        K storeKey = _keySerializer.deserialize(key);
        return _store.delete(storeKey);
    }
}
