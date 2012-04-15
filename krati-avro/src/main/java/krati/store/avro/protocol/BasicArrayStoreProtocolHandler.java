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

import java.nio.ByteOrder;

import krati.io.serializer.IntSerializer;
import krati.store.ArrayStore;

/**
 * BasicArrayStoreProtocolHandler defines the interface between an {@link ArrayStore} and the transport protocol
 * (see {@link krati.store.avro.protocol.Protocols#getProtocol() Protocols}).
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class BasicArrayStoreProtocolHandler implements StoreProtocolHandler {
    protected final ArrayStore _store;
    protected final IntSerializer _keySerializer;
    
    /**
     * Creates a new ArrayStoreProtocolHandler instance.
     * 
     * @param store - the underlying store
     */
    public BasicArrayStoreProtocolHandler(ArrayStore store) {
        this._store = store;
        this._keySerializer = new IntSerializer(ByteOrder.BIG_ENDIAN);
    }
    
    /**
     * Creates a new ArrayStoreProtocolHandler instance.
     * 
     * @param store         - the underlying store
     * @param keySerializer - the int key serializer 
     */
    public BasicArrayStoreProtocolHandler(ArrayStore store, IntSerializer keySerializer) {
        this._store = store;
        this._keySerializer = keySerializer;
    }
    
    /**
     * @return the underlying {@link krati.store.ArrayStore}.
     */
    public final ArrayStore getStore() {
        return _store;
    }
    
    /**
     * @return the next System Change Number (SCN) in an ever-increasing order.
     */
    protected long nextScn() {
        return System.currentTimeMillis();
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
        int index = _keySerializer.intValue(key);
        return _store.get(index);
    }
    
    @Override
    public boolean put(byte[] key, byte[] value) throws Exception {
        int index = _keySerializer.intValue(key);
        _store.set(index, value, nextScn());
        return true;
    }
    
    @Override
    public boolean delete(byte[] key) throws Exception {
        int index = _keySerializer.intValue(key);
        if(!_store.hasIndex(index)) {
            return false;
        }
        
        _store.delete(index, nextScn());
        return true;
    }
}
