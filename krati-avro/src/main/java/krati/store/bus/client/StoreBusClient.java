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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import krati.retention.Position;
import krati.store.bus.StoreBus;

/**
 * StoreBusClient
 * 
 * @author jwu
 * @since 10/04, 2011
 */
public interface StoreBusClient<K, V> extends StoreBus<K, V> {
    
    public Position getPosition(String positionStr);
    
    public Map<K, V> get(Collection<K> keys) throws Exception;
    
    public Position syncUp(Position position, List<K> list) throws Exception;
    
    public Position syncUp(Position position, Map<K, V> map) throws Exception;
    
    public String getProperty(String key) throws Exception;
    
    public String setProperty(String key, String value) throws Exception;
    
}
