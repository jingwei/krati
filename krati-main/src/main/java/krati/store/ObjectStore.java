/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
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

package krati.store;

/**
 * ObjectStore
 * 
 * @author jwu
 *
 * @param <K> Key
 * @param <V> Value
 */
public interface ObjectStore<K, V> extends DataStore<K, V> {
    
    /**
     * Gets an object in the form of byte array from the store.
     * 
     * @param key  the retrieving key. 
     * @return the retrieved object in raw bytes.
     */
    public byte[] getBytes(K key);
    
    /**
     * Gets an object in the form of byte array from the store.
     * 
     * @param keyBytes  the retrieving key in raw bytes. 
     * @return the retrieved object in raw bytes.
     */
    public byte[] getBytes(byte[] keyBytes);
    
}
