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

package krati.store.avro.protocol;

import java.nio.ByteBuffer;
import java.util.Properties;


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
 * StoreResponder
 * 
 * @author jwu
 * @since 09/23, 2011
 */
public class StoreResponder extends GenericResponder {
    private final static Logger _logger = Logger.getLogger(StoreResponder.class);
    private final Properties _properties = new Properties();
    private final StoreProtocolHandler _handler;
    
    public StoreResponder(StoreProtocolHandler handler) {
        super(Protocols.getProtocol());
        this._handler = handler;
    }
    
    public final StoreProtocolHandler getHandler() {
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
        
        if (msgName.equals(ProtocolConstants.MSG_GET)) {
            ByteBuffer key = (ByteBuffer)record.get("key");
            byte[] bytes = _handler.get(key.array());
            return bytes == null ? null : ByteBuffer.wrap(bytes);
        }
        
        if (msgName.equals(ProtocolConstants.MSG_PUT)) {
            GenericRecord kv = (GenericRecord)record.get("kv");
            
            try {
                ByteBuffer key = (ByteBuffer)kv.get("key");
                ByteBuffer value = (ByteBuffer)kv.get("value");
                return _handler.put(key.array(), value.array());
            } catch(Exception e) {
                _logger.error("put failed: " + kv, e);
            }
            
            return false;
        }
        
        if (msgName.equals(ProtocolConstants.MSG_DEL)) {
            ByteBuffer key = (ByteBuffer)record.get("key");
            
            try {
                return _handler.delete(key.array());
            } catch(Exception e) {
                _logger.error("delete failed", e);
            }
            
            return false;
        }
        
        if (msgName.equals(ProtocolConstants.MSG_MGET)) {
            GenericArray<ByteBuffer> keys = (GenericArray<ByteBuffer>)record.get("keys");
            
            Schema schemaKV = getLocal().getType(ProtocolConstants.TYPE_KeyValue);
            Schema schema = getLocal().getMessages().get(ProtocolConstants.MSG_MGET).getRequest().getField("keys").schema();
            GenericArray<GenericRecord> array = new GenericData.Array<GenericRecord>(keys.size(), schema); 
            
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
        }
        
        if (msgName.equals(ProtocolConstants.MSG_MPUT)) {
            boolean ret = false;
            GenericArray<GenericRecord> array = (GenericArray<GenericRecord>)record.get("kvList");
            for(GenericRecord kv : array) {
                ByteBuffer key = (ByteBuffer)kv.get("key");
                ByteBuffer value = (ByteBuffer)kv.get("value");
                
                try {
                    if(value == null) {
                        if(_handler.delete(key.array())) {
                            ret = true;
                        }
                    } else {
                        if(_handler.put(key.array(), value.array())) {
                            ret = true;
                        }
                    }
                } catch(Exception e) {
                    _logger.error("put failed: " + kv, e);
                }
            }
            
            return ret;
        }
        
        if (msgName.equals(ProtocolConstants.MSG_MDEL)) {
            boolean ret = false;
            GenericArray<ByteBuffer> array = (GenericArray<ByteBuffer>)record.get("keys");
            for(ByteBuffer key : array) {
                try {
                    if(_handler.delete(key.array())) {
                        ret = true;
                    }
                } catch(Exception e) {
                    _logger.error("delete failed", e);
                }
            }
            
            return ret;
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
                    Object obj = _properties.get(key);
                    return (obj == null) ? null : new Utf8(obj.toString());
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
