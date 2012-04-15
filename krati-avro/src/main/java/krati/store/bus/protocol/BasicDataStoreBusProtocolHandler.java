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

package krati.store.bus.protocol;

import java.util.Map;

import krati.retention.Event;
import krati.retention.Position;
import krati.retention.SimplePosition;
import krati.retention.clock.Clock;
import krati.store.avro.protocol.ProtocolConstants;
import krati.store.bus.StoreBus;

/**
 * BasicDataStoreBusProtocolHandler
 * 
 * @author jwu
 * @since 10/04, 2011
 */
public class BasicDataStoreBusProtocolHandler implements StoreBusProtocolHandler {
    protected final StoreBus<byte[], byte[]> _storeBus;
    
    public BasicDataStoreBusProtocolHandler(StoreBus<byte[], byte[]> storeBus) {
        this._storeBus = storeBus;
    }
    
    @Override
    public byte[] get(byte[] key) throws Exception {
        return _storeBus.get(key);
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
        return _storeBus.get(position, map);    
    }
}
