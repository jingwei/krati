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

package krati.store.avro.client;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import krati.io.Serializer;
import krati.store.avro.protocol.ProtocolConstants;
import krati.store.avro.protocol.StoreDirective;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;

/**
 * StoreClientImpl
 * 
 * @author jwu
 * @since 09/28, 2011
 */
public class StoreClientImpl<K, V> extends BaseClient<K, V> implements StoreClient<K, V> {
    private final static Logger _logger = Logger.getLogger(StoreClientImpl.class);
    
    public StoreClientImpl(String source,
                           Serializer<K> keySerializer,
                           Serializer<V> valueSerializer,
                           TransceiverFactory transceiverFactory) {
        super(source, keySerializer, valueSerializer, transceiverFactory);
        if (!init()) {
            getLogger().error("Initialization failed");
        }
    }
    
    protected Logger getLogger() {
        return _logger;
    }
    
    @Override
    public V get(K key) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_GET).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        req.put("src", _sourceUtf8);
        req.put("key", serializeKey(key));
        
        ByteBuffer bb = (ByteBuffer)send(ProtocolConstants.MSG_GET, req);
        return deserializeValue(bb);
    }
    
    @Override
    public boolean put(K key, V value) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        if(value == null) {
            return delete(key);
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_PUT).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        GenericRecord kv = new GenericData.Record(_protocol.getType(ProtocolConstants.TYPE_KeyValue));
        kv.put("key", serializeKey(key));
        kv.put("value", serializeValue(value));
        
        req.put("src", _sourceUtf8);
        req.put("kv", kv);
        
        return (Boolean)send(ProtocolConstants.MSG_PUT, req);
    }
    
    @Override
    public boolean delete(K key) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_DEL).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        req.put("src", _sourceUtf8);
        req.put("key", serializeKey(key));
        
        return (Boolean)send(ProtocolConstants.MSG_DEL, req);
    }
    
    @Override @SuppressWarnings("unchecked")
    public Map<K, V> get(Collection<K> keys) throws Exception {
        if(keys == null) {
            throw new NullPointerException("keys");
        }
        if(keys.size() == 0) {
            return new HashMap<K, V>();
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_MGET).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        GenericArray<ByteBuffer> array = new GenericData.Array<ByteBuffer>(keys.size(), schema.getField("keys").schema()); 
        
        for(K key : keys) {
            if(key != null) {
                array.add(serializeKey(key));
            }
        }
        
        if(array.size() == 0) {
            return new HashMap<K, V>();
        }
        
        req.put("src", _sourceUtf8);
        req.put("keys", array);
        
        GenericArray<GenericRecord> response = (GenericArray<GenericRecord>)send(ProtocolConstants.MSG_MGET, req); 
        
        Map<K, V> map = new HashMap<K, V>();
        for(GenericRecord kv : response) {
            K key = deserializeKey((ByteBuffer)kv.get("key"));
            V value = deserializeValue((ByteBuffer)kv.get("value"));
            map.put(key, value);
        }
        
        return map;
    }
    
    @Override
    public boolean put(Map<K, V> map) throws Exception {
        if(map == null) {
            throw new NullPointerException("map");
        }
        if(map.size() == 0) {
            return false;
        }
        
        Schema schemaKV = _protocol.getType(ProtocolConstants.TYPE_KeyValue);
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_MPUT).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        GenericArray<GenericRecord> array = new GenericData.Array<GenericRecord>(map.size(), schema.getField("kvList").schema()); 
        
        for(Map.Entry<K, V> e : map.entrySet()) {
            GenericRecord item = new GenericData.Record(schemaKV);
            K key = e.getKey();
            V value = e.getValue();
            item.put("key", serializeKey(key));
            item.put("value", value == null ? null : serializeValue(value));
            array.add(item);
        }
        
        if(array.size() == 0) {
            return false;
        }
        
        req.put("src", _sourceUtf8);
        req.put("kvList", array);
        
        return (Boolean)send(ProtocolConstants.MSG_MPUT, req);
    }
    
    @Override 
    public boolean delete(Collection<K> keys) throws Exception {
        if(keys == null) {
            throw new NullPointerException("keys");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_MDEL).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        GenericArray<ByteBuffer> array = new GenericData.Array<ByteBuffer>(keys.size(), schema.getField("keys").schema()); 
        
        for(K key : keys) {
            if(key != null) {
                array.add(serializeKey(key));
            }
        }
        
        if(array.size() == 0) {
            return false;
        }
        
        req.put("src", _sourceUtf8);
        req.put("keys", array);
        
        return (Boolean)send(ProtocolConstants.MSG_MDEL, req);
    }
    
    @Override
    public String getProperty(String key) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_META).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", ProtocolConstants.OPT_GET_PROPERTY_UTF8);
        req.put("key", new Utf8(key));
        
        Utf8 res = (Utf8)send(ProtocolConstants.MSG_META, req);
        return res == null ? null : res.toString();
    }
    
    @Override
    public String setProperty(String key, String value) throws Exception {
        if(key == null) {
            throw new NullPointerException("key");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_META).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        if(value != null) {
            req.put("opt", ProtocolConstants.OPT_SET_PROPERTY_UTF8);
            req.put("key", new Utf8(key));
            req.put("value", new Utf8(value));
        } else {
            req.put("opt", ProtocolConstants.OPT_DEL_PROPERTY_UTF8);
            req.put("key", new Utf8(key));
        }
        
        Utf8 res = (Utf8)send(ProtocolConstants.MSG_META, req);
        return res == null ? null : res.toString();
    }
    
    @Override
    public String send(StoreDirective directive) throws Exception {
        if(directive == null) {
            throw new NullPointerException("directive");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_META).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", new Utf8(directive.toString()));
        
        Utf8 res = (Utf8)send(ProtocolConstants.MSG_META, req);
        return res == null ? null : res.toString();
    }
}
