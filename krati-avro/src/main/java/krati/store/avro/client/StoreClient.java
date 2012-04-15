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

import java.util.Collection;
import java.util.Map;

import krati.store.StoreReader;
import krati.store.StoreWriter;
import krati.store.avro.protocol.StoreDirective;

/**
 * StoreClient
 * 
 * @author jwu
 * @since 09/20, 2011
 */
public interface StoreClient<K, V> extends StoreReader<K, V>, StoreWriter<K, V> {
    
    public String getSource();
    
    public Map<K, V> get(Collection<K> keys) throws Exception;
    
    public boolean put(Map<K, V> map) throws Exception;
    
    public boolean delete(Collection<K> keys) throws Exception;
    
    public String getProperty(String key) throws Exception;
    
    public String setProperty(String key, String value) throws Exception;
    
    public String send(StoreDirective directive) throws Exception;
}
