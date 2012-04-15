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

package krati.store.avro.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericRequestor;
import org.apache.avro.util.Utf8;

import krati.io.Serializer;
import krati.store.avro.protocol.Protocols;

/**
 * BaseClient
 * 
 * @author jwu
 * @since 09/27, 2011
 */
public class BaseClient<K, V> {
    protected String _source;
    protected Utf8 _sourceUtf8;
    protected Protocol _protocol;
    protected Serializer<K> _keySerializer;
    protected Serializer<V> _valueSerializer;
    protected TransceiverFactory _transceiverFactory;
    
    protected BaseClient(String source,
                         Serializer<K> keySerializer,
                         Serializer<V> valueSerializer,
                         TransceiverFactory transceiverFactory) {
        if(source == null) {
            throw new NullPointerException("source");
        }
        if(transceiverFactory == null) {
            throw new NullPointerException("transceiverFactory");
        }
        if((_protocol = Protocols.getProtocol()) == null) {
            throw new IllegalStateException("Protocol not found");
        }
        
        this._source = source;
        this._keySerializer = keySerializer;
        this._valueSerializer = valueSerializer;
        this._transceiverFactory = transceiverFactory;
        this._sourceUtf8 = new Utf8(source);
        this.init();
    }
    
    protected boolean init() {
        return true;
    }
    
    protected ByteBuffer serializeKey(K key) {
        return ByteBuffer.wrap(_keySerializer.serialize(key));
    }
    
    protected ByteBuffer serializeValue(V value) {
        return ByteBuffer.wrap(_valueSerializer.serialize(value));
    }
    
    protected K deserializeKey(ByteBuffer bb) {
        return _keySerializer.deserialize(bb.array());
    }
    
    protected V deserializeValue(ByteBuffer bb) {
        return bb == null ? null : _valueSerializer.deserialize(bb.array());
    }
    
    protected Object send(String msg, Object request) throws IOException {
        GenericRequestor requestor = new GenericRequestor(_protocol, _transceiverFactory.newTransceiver());
        return requestor.request(msg, request);
    }
    
    public final String getSource() {
        return _source;
    }
    
    public final Protocol getProtocol() {
        return _protocol;
    }
    
    public final Serializer<K> getKeySerializer() {
        return _keySerializer;
    }
    
    public final Serializer<V> getValueSerializer() {
        return _valueSerializer;
    }
    
    public final TransceiverFactory getTransceiverFactory() {
        return _transceiverFactory;
    }
}
