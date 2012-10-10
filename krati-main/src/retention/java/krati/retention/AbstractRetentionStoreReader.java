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

package krati.retention;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Provides a trivial implementation of get(pos, map), that uses get(pos, list) and get
 * 
 * @author spike(alperez)
 * @since 07/31, 2012
 * @param <K> Key
 * @param <V> Value
 */
public abstract class AbstractRetentionStoreReader<K, V> implements RetentionStoreReader<K, V> {
    private final static Logger logger = Logger.getLogger(AbstractRetentionStoreReader.class);
    
    /**
     * Gets a number of value events starting from a give position in the Retention.
     * The number of events is determined internally by the Retention and it is
     * up to the batch size.   
     * 
     * @param pos - the retention position from where events will be read
     * @param map - the result map (keys to value events) to fill in 
     * @return the next position from where new events will be read.
     */
    public Position get(Position pos, Map<K, Event<V>> map) {
        ArrayList<Event<K>> list = new ArrayList<Event<K>>(1000);
        Position nextPos = get(pos, list);
        
        for(Event<K> evt : list) {
            K key = evt.getValue();
            if(key != null) {
                try {
                    V value = get(key);
                    map.put(key, new SimpleEvent<V>(value, evt.getClock()));
                } catch(Exception e) {
                    logger.warn(e.getMessage());
                }
            }
        }
        
        return nextPos;
    }
}
