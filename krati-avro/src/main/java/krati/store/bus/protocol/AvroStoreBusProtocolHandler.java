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

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import krati.io.Serializer;
import krati.retention.Event;
import krati.retention.Position;
import krati.retention.SimpleEvent;
import krati.retention.SimplePosition;
import krati.retention.clock.Clock;
import krati.store.avro.protocol.ProtocolConstants;
import krati.store.bus.AvroStoreBus;

/**
 * AvroStoreBusProtocolHandler
 * 
 * @author jwu
 * @since 10/04, 2011
 */
public class AvroStoreBusProtocolHandler<K> implements StoreBusProtocolHandler {
    protected final AvroStoreBus<K> _storeBus;
    protected final Serializer<K> _keySerializer;
    protected final Serializer<GenericRecord> _valueSerializer;
    
    public AvroStoreBusProtocolHandler(AvroStoreBus<K> storeBus) {
        this._storeBus = storeBus;
        this._keySerializer = storeBus.getStore().getKeySerializer();
        this._valueSerializer = storeBus.getStore().getValueSerializer();
    }
    
    @Override
    public byte[] get(byte[] key) throws Exception {
        K storeKey = _keySerializer.deserialize(key);
        GenericRecord storeValue = _storeBus.get(storeKey);
        return storeValue == null ? null : _valueSerializer.serialize(storeValue);
    }
    
    @Override
    public Position getPosition() {
        return _storeBus.getPosition();
    }

    @Override
    public Position getPosition(Clock clock) {
        return _storeBus.getPosition(clock);
    }
    
    @Override
    public Position getPosition(String positionStr) {
        return SimplePosition.parsePosition(positionStr);
    }
    
    @Override
    public String meta(String opt, String key, String value) throws Exception {
        StoreBusDirective directive = StoreBusDirective.valueOf(opt);
        if(directive == StoreBusDirective.Position) {
            if(key == null) {
                return getPosition().toString();
            } else {
                Clock clock = Clock.parseClock(key);
                return getPosition(clock).toString();
            }
        }
        
        // no-operation
        return ProtocolConstants.NOP;
    }
    
    @Override
    public Position syncUp(Position position, Map<byte[], Event<byte[]>> map) {
        Map<K, Event<GenericRecord>> inputMap = new HashMap<K, Event<GenericRecord>>();
        Position nextPosition = _storeBus.get(position, inputMap);
        
        for(Map.Entry<K, Event<GenericRecord>> e : inputMap.entrySet()) {
            byte[] key = _keySerializer.serialize(e.getKey());
            
            GenericRecord record = e.getValue().getValue();
            byte[] value = record == null ? null : _valueSerializer.serialize(record);
            map.put(key, new SimpleEvent<byte[]>(value, e.getValue().getClock()));
        }
        
        return nextPosition;
    }
}
