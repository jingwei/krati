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

import krati.store.DataStore;

/**
 * BasicDataStoreProtocolHandler defines the interface between an {@link DataStore} with keys and values in the
 * form of byte array and the transport protocol (see {@link krati.store.avro.protocol.Protocols#getProtocol() Protocols}).
 * 
 * @author jwu
 * @since 09/23, 2011
 */
public class BasicDataStoreProtocolHandler implements StoreProtocolHandler {
    protected final DataStore<byte[], byte[]> _store;
    
    /**
     * Creates a new DataStoreProtocolHandler instance.
     * 
     * @param store - the underlying store
     */
    public BasicDataStoreProtocolHandler(DataStore<byte[], byte[]> store) {
        this._store = store;
    }
    
    /**
     * @return the underlying {@link krati.store.DataStore}.
     */
    public final DataStore<byte[], byte[]> getStore() {
        return _store;
    }
    
    @Override
    public String meta(String opt, String key, String value) throws Exception {
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
        return _store.get(key);
    }
    
    @Override
    public boolean put(byte[] key, byte[] value) throws Exception {
        return _store.put(key, value);
    }
    
    @Override
    public boolean delete(byte[] key) throws Exception {
        return _store.delete(key);
    }
}
