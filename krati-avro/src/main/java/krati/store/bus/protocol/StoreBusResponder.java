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

package krati.store.bus.protocol;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import krati.retention.Event;
import krati.retention.Position;
import krati.retention.clock.Clock;
import krati.store.avro.protocol.ProtocolConstants;
import krati.store.avro.protocol.Protocols;
import krati.store.avro.protocol.StoreResponder;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericResponder;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;

/**
 * StoreBusResponder
 * 
 * @author jwu
 * @since 10/04, 2011
 */
public class StoreBusResponder extends GenericResponder {
    private final static Logger _logger = Logger.getLogger(StoreResponder.class);
    private final Properties _properties = new Properties();
    private final StoreBusProtocolHandler _handler;
    
    public StoreBusResponder(StoreBusProtocolHandler handler) {
        super(Protocols.getProtocol());
        this._handler = handler;
    }
    
    public final StoreBusProtocolHandler getHandler() {
        return _handler;
    }
    
    public final Properties getProperties() {
        return _properties;
    }
    
    public final String getProperty(String key) {
        return _properties.getProperty(key);
    }
    
    public final String getProperty(String key, String defaultValue) {
        return _properties.getProperty(key, defaultValue);
    }
    
    public final Object setProperty(String key, String value) {
        return _properties.setProperty(key, value);
    }
    
    @Override @SuppressWarnings("unchecked")
    public Object respond(Protocol.Message message, Object request) {
        GenericRecord record = (GenericRecord)request;
        String msgName = message.getName();

        if (msgName.equals(ProtocolConstants.MSG_SYNC)) {
            String positionStr = record.get("position").toString();
            Position position = _handler.getPosition(positionStr);
            
            // Check if clock value needs to be transferred
            Utf8 opt = (Utf8)record.get("opt");
            boolean clockNotNeeded = (opt == null) ? false : opt.toString().equals(StoreBusOptions.OPT_NO_CLOCK);
            
            Map<byte[], Event<byte[]>> map = new HashMap<byte[], Event<byte[]>>(1024);
            Position nextPosition = _handler.syncUp(position, map);
            
            Schema schemaKVC = getLocal().getType(ProtocolConstants.TYPE_KeyValueClock);
            Schema schema1 = getLocal().getType(ProtocolConstants.TYPE_SyncResultSet);
            Schema schema2 = schema1.getField("results").schema();
            GenericArray<GenericRecord> array = new GenericData.Array<GenericRecord>(map.size(), schema2); 
            
            for(Map.Entry<byte[], Event<byte[]>> e : map.entrySet()) {
                GenericRecord kvc = new GenericData.Record(schemaKVC);
                kvc.put("key", ByteBuffer.wrap(e.getKey()));
                
                byte[] value = e.getValue().getValue();
                kvc.put("value", value == null ? null : ByteBuffer.wrap(value));
                
                Clock clock = clockNotNeeded ? null : e.getValue().getClock();
                kvc.put("clock", clock == null ? null : ByteBuffer.wrap(clock.toByteArray()));
                
                array.add(kvc);
            }
            
            GenericRecord response = new GenericData.Record(schema1);
            response.put("results", array);
            response.put("position", new Utf8(nextPosition.toString()));
            
            return response;
        }
        
        if (msgName.equals(ProtocolConstants.MSG_GET)) {
            try {
                ByteBuffer key = (ByteBuffer)record.get("key");
                byte[] bytes = _handler.get(key.array());
                return bytes == null ? null : ByteBuffer.wrap(bytes);
            } catch(Exception e) {
                throw new AvroRuntimeException(e);
            }
        }
        
        if (msgName.equals(ProtocolConstants.MSG_MGET)) {
            GenericArray<ByteBuffer> keys = (GenericArray<ByteBuffer>)record.get("keys");
            
            Schema schemaKV = getLocal().getType(ProtocolConstants.TYPE_KeyValue);
            Schema schema = getLocal().getMessages().get(ProtocolConstants.MSG_MGET).getRequest().getField("keys").schema();
            GenericArray<GenericRecord> array = new GenericData.Array<GenericRecord>(keys.size(), schema); 
            
            try {
                // Include only non-null key and non-null value in response
                for(ByteBuffer key : keys) {
                    if(key != null) {
                        byte[] bytes = _handler.get(key.array());
                        if(bytes != null) {
                             GenericRecord kv = new GenericData.Record(schemaKV);
                             kv.put("key", key);
                             kv.put("value", ByteBuffer.wrap(bytes));
                             array.add(kv);
                        }
                    }
                }
                
                return array;
            } catch(Exception e) {
                throw new AvroRuntimeException(e);
            }
        }
        
        if (msgName.equals(ProtocolConstants.MSG_META)) {
            Utf8 metaOpt = (Utf8)record.get("opt");
            Utf8 metaKey = (Utf8)record.get("key");
            Utf8 metaValue = (Utf8)record.get("value");
            
            String opt = metaOpt.toString();
            String key = (metaKey == null) ? null : metaKey.toString();
            String value = (metaValue == null) ? null : metaValue.toString();
            
            try {
                String ret = _handler.meta(opt, key, value);
                return ret == null ? null : new Utf8(ret);
            } catch(IllegalArgumentException e) {
                if(opt.equals(ProtocolConstants.OPT_SET_PROPERTY)) {
                    Object obj = _properties.setProperty(key, value);
                    return (obj == null) ? null : new Utf8(obj.toString());
                } else if(opt.equals(ProtocolConstants.OPT_DEL_PROPERTY)) {
                    Object obj = _properties.remove(key);
                    return (obj == null) ? null : new Utf8(obj.toString());
                } else if(opt.equals(ProtocolConstants.OPT_GET_PROPERTY)) {
                    if(key == null) {
                        return null;
                    }
                    String obj = _properties.getProperty(key);
                    return (obj == null) ? null : new Utf8(obj);
                } else {
                    return ProtocolConstants.NOP_UTF8; 
                }
            } catch(Exception e) {
                _logger.error("Failed on meta: " + opt + " key=" + key + " value=" + value, e);
                return ProtocolConstants.ERR_UTF8;
            }
        }
        
        throw new AvroRuntimeException("Unexpected message: " + msgName);
    }
}
