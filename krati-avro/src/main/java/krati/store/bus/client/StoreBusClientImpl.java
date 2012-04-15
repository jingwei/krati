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

package krati.store.bus.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import krati.io.Serializer;
import krati.retention.Event;
import krati.retention.Position;
import krati.retention.SimpleEvent;
import krati.retention.SimplePosition;
import krati.retention.clock.Clock;
import krati.store.avro.client.BaseClient;
import krati.store.avro.client.TransceiverFactory;
import krati.store.avro.protocol.ProtocolConstants;
import krati.store.bus.protocol.StoreBusDirective;
import krati.store.bus.protocol.StoreBusOptions;

/**
 * StoreEventsClientImpl
 * 
 * @author jwu
 * @since 10/04, 2011
 */
public class StoreBusClientImpl<K, V> extends BaseClient<K, V> implements StoreBusClient<K, V> {
    
    public StoreBusClientImpl(String source,
                              Serializer<K> keySerializer,
                              Serializer<V> valueSerializer,
                              TransceiverFactory transceiverFactory) {
        super(source, keySerializer, valueSerializer, transceiverFactory);
    }
    
    protected Clock deserializeClock(ByteBuffer bb) {
        return Clock.parseClock(bb.array());
    }
    
    @Override
    public Position getPosition() {
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_META).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", new Utf8(StoreBusDirective.Position.toString()));
        req.put("key", null);
        req.put("value", null);
        
        try {
            Utf8 res = (Utf8)send(ProtocolConstants.MSG_META, req);
            return res == null ? null : getPosition(res.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Position getPosition(Clock clock) {
        if(clock == null) {
            throw new NullPointerException("clock");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_META).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", new Utf8(StoreBusDirective.Position.toString()));
        req.put("key", new Utf8(clock.toString()));
        req.put("value", null);
        
        try {
            Utf8 res = (Utf8)send(ProtocolConstants.MSG_META, req);
            return res == null ? null : getPosition(res.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Position getPosition(String positionStr) {
        return SimplePosition.parsePosition(positionStr);
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
    public Position get(Position position, Map<K, Event<V>> map) {
        if(position == null) {
            throw new NullPointerException("position");
        }
        if(map == null) {
            throw new NullPointerException("map");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_SYNC).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", null);
        req.put("position", new Utf8(position.toString()));
        
        try {
            GenericRecord response = (GenericRecord)send(ProtocolConstants.MSG_SYNC, req);
            
            @SuppressWarnings("unchecked")
            GenericArray<GenericRecord> items = (GenericArray<GenericRecord>)response.get("results");
            
            for(GenericRecord item : items) {
                K key = deserializeKey((ByteBuffer)item.get("key"));
                V value = deserializeValue((ByteBuffer)item.get("value"));
                Clock clock = deserializeClock((ByteBuffer)item.get("clock"));
                map.put(key, new SimpleEvent<V>(value, clock));
            }
            
            return getPosition(response.get("position").toString());
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }
    
    @Override
    public Position get(Position position, List<Event<K>> list) {
        if(position == null) {
            throw new NullPointerException("position");
        }
        if(list == null) {
            throw new NullPointerException("list");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_SYNC).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", null);
        req.put("position", new Utf8(position.toString()));
        
        try {
            GenericRecord response = (GenericRecord)send(ProtocolConstants.MSG_SYNC, req);
            
            @SuppressWarnings("unchecked")
            GenericArray<GenericRecord> items = (GenericArray<GenericRecord>)response.get("results");
            
            for(GenericRecord item : items) {
                K key = deserializeKey((ByteBuffer)item.get("key"));
                Clock clock = deserializeClock((ByteBuffer)item.get("clock"));
                list.add(new SimpleEvent<K>(key, clock));
            }
            
            return getPosition(response.get("position").toString());
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }

    @Override
    public Position syncUp(Position position, Map<K, V> map) {
        if(position == null) {
            throw new NullPointerException("position");
        }
        if(map == null) {
            throw new NullPointerException("map");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_SYNC).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", StoreBusOptions.OPT_NO_CLOCK_UTF8);
        req.put("position", new Utf8(position.toString()));
        
        try {
            GenericRecord response = (GenericRecord)send(ProtocolConstants.MSG_SYNC, req);
            
            @SuppressWarnings("unchecked")
            GenericArray<GenericRecord> items = (GenericArray<GenericRecord>)response.get("results");
            
            for(GenericRecord item : items) {
                K key = deserializeKey((ByteBuffer)item.get("key"));
                V value = deserializeValue((ByteBuffer)item.get("value"));
                map.put(key, value);
            }
            
            return getPosition(response.get("position").toString());
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }

    @Override
    public Position syncUp(Position position, List<K> list) {
        if(position == null) {
            throw new NullPointerException("position");
        }
        if(list == null) {
            throw new NullPointerException("list");
        }
        
        Schema schema = _protocol.getMessages().get(ProtocolConstants.MSG_SYNC).getRequest();
        GenericRecord req = new GenericData.Record(schema);
        
        req.put("src", _sourceUtf8);
        req.put("opt", StoreBusOptions.OPT_NO_CLOCK_UTF8);
        req.put("position", new Utf8(position.toString()));
        
        try {
            GenericRecord response = (GenericRecord)send(ProtocolConstants.MSG_SYNC, req);
            
            @SuppressWarnings("unchecked")
            GenericArray<GenericRecord> items = (GenericArray<GenericRecord>)response.get("results");
            
            for(GenericRecord item : items) {
                K key = deserializeKey((ByteBuffer)item.get("key"));
                list.add(key);
            }
            
            return getPosition(response.get("position").toString());
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
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
            req.put("key", new Utf8(ProtocolConstants.OPT_DEL_PROPERTY + key));
        }
        
        Utf8 res = (Utf8)send(ProtocolConstants.MSG_META, req);
        return res == null ? null : res.toString();
    }
}
