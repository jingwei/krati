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

import java.util.Map;

import krati.retention.Event;
import krati.retention.Position;
import krati.retention.clock.Clock;

/**
 * StoreBusProtocolHandler
 * 
 * @author jwu
 * @since 10/04, 2011
 */
public interface StoreBusProtocolHandler {
    
    /**
     * Gets the value to which the specified <code>key</code> is mapped.
     * 
     * @param key - the key
     * @return the value associated with the <code>key</code>,
     *         or <code>null</code> if the <code>key</code> is not known.
     */
    public byte[] get(byte[] key) throws Exception;
    
    /**
     * Gets the most recent store bus position.
     * 
     * @return the current store bus position.
     */
    public Position getPosition();
    
    /**
     * Gets the store bus position mapped to a user specified clock.
     * 
     * @param clock - the clock
     * @return a store bus position
     */
    public Position getPosition(Clock clock);
    
    /**
     * Gets the store bus position from its string representation.
     * 
     * @param positionStr
     * @return a store bus position
     */
    public Position getPosition(String positionStr);
    
    /**
     * Gets the meta value according to the specified meta key.
     * 
     * @param opt   - meta option
     * @param key   - meta key
     * @param value - meta value
     * @return the meta value associated with the meta key.
     * @throws Exception if the meta key can not be handled for any reasons.
     */
    public String meta(String opt, String key, String value) throws Exception;
    
    /**
     * Sync up update events from the specified position.
     * 
     * @param position - the position from where update events are to be collected.
     * @param map      - the map to which update events are to be added.
     * @return the next position from where new update events are to be collected.
     */
    public Position syncUp(Position position,  Map<byte[], Event<byte[]>> map);
    
}
