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

import java.util.Map;

import krati.store.StoreReader;

/**
 * RetentionStoreReader
 * 
 * @param <K> Key
 * @param <V> Value
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/23, 2011 - Created <br/>
 * 09/21, 2011 - Added interface StoreReader <br/>
 */
public interface RetentionStoreReader<K, V> extends RetentionClient<K>, StoreReader<K, V> {
    
    /**
     * @return the data source of this RetentionStoreReader.
     */
    public String getSource();
    
    /**
     * Gets a number of value events starting from a give position in the Retention.
     * The number of events is determined internally by the Retention and it is
     * up to the batch size.   
     * 
     * @param pos - the retention position from where events will be read
     * @param map - the result map (keys to value events) to fill in 
     * @return the next position from where new events will be read.
     */
    public Position get(Position pos, Map<K, Event<V>> map);
}
